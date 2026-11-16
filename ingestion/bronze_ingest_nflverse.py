"""
GridironLakehouse | Bronze layer ingestion
==========================================

Lands nflverse release files in a Unity Catalog volume, then loads them into raw
Delta tables:

    <catalog>.<schema>.bronze_nfl_pbp      one row per play, partitioned by season
    <catalog>.<schema>.bronze_nfl_players  player master. Play-by-play carries player
                                           ids and short names but no positions, so
                                           dim_players needs this file as well.

Design notes
------------
* Spark has no HTTP filesystem, so each file is downloaded on the driver, checked
  for completeness, and copied to a landing volume before Spark reads it.
* Parquet is self-describing: Spark infers every column from the file footers and
  no schema is hand-maintained here. nflverse does not keep physical types stable
  across seasons, though (``goal_to_go`` is INT32 in 2021-2023 and DOUBLE in the
  other seasons), so each season is read on its own and the frames are widened to
  one common schema before they are unioned.
* Loads are idempotent. The play-by-play table is partitioned by ``season`` and a
  run atomically replaces only the seasons it downloaded (``replaceWhere``). The
  weekly in-season refresh is therefore ``--start-season 2026 --end-season 2026``,
  and a failed run is fixed by running it again.

Usage
-----
Run as a Databricks job (Python script task) or from a notebook, on a workspace
with Unity Catalog and outbound HTTPS access to github.com:

    python bronze_ingest_nflverse.py --catalog workspace --schema bronze \
        --start-season 2020 --end-season 2026
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from functools import reduce

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

NFLVERSE_RELEASES = "https://github.com/nflverse/nflverse-data/releases/download"
PBP_URL = NFLVERSE_RELEASES + "/pbp/play_by_play_{season}.parquet"
PLAYERS_URL = NFLVERSE_RELEASES + "/players/players.parquet"

PBP_TABLE = "bronze_nfl_pbp"
PLAYERS_TABLE = "bronze_nfl_players"
LANDING_VOLUME = "landing"

FIRST_NFLVERSE_SEASON = 1999
HTTP_TIMEOUT_SECONDS = (10, 120)  # (connect, read)
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
PARQUET_MAGIC = b"PAR1"

# Integral types from narrowest to widest, used when reconciling schema drift.
INTEGRAL_TYPE_ORDER = (T.ByteType, T.ShortType, T.IntegerType, T.LongType)

logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("gridiron.bronze")
log.setLevel(logging.INFO)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class IngestConfig:
    """Where to load from and to. Built from command-line arguments."""

    catalog: str
    schema: str
    start_season: int
    end_season: int

    @property
    def seasons(self) -> list[int]:
        return list(range(self.start_season, self.end_season + 1))

    @property
    def landing_root(self) -> str:
        """Volume folder that holds the most recent copy of every downloaded file."""
        return f"/Volumes/{self.catalog}/{self.schema}/{LANDING_VOLUME}/nflverse"

    def qualified(self, name: str) -> str:
        """Fully qualified, backtick-quoted name of an object in the bronze schema."""
        return f"`{self.catalog}`.`{self.schema}`.`{name}`"


def parse_args(argv: list[str] | None = None) -> IngestConfig:
    parser = argparse.ArgumentParser(description="Load nflverse data into bronze Delta tables.")
    parser.add_argument("--catalog", default="workspace", help="Unity Catalog catalog (default: workspace)")
    parser.add_argument("--schema", default="bronze", help="Schema for the bronze tables (default: bronze)")
    parser.add_argument("--start-season", type=int, default=2020, help="First season to load (default: 2020)")
    parser.add_argument("--end-season", type=int, default=2026, help="Last season to load (default: 2026)")

    # parse_known_args: notebooks and job launchers put entries of their own in argv.
    args, _ = parser.parse_known_args(argv)

    if not FIRST_NFLVERSE_SEASON <= args.start_season <= args.end_season:
        parser.error(
            f"need {FIRST_NFLVERSE_SEASON} <= start-season <= end-season, "
            f"got {args.start_season}..{args.end_season}"
        )
    for identifier in (args.catalog, args.schema):
        if not identifier or "`" in identifier or "/" in identifier:
            parser.error(f"invalid catalog or schema name: {identifier!r}")

    return IngestConfig(args.catalog, args.schema, args.start_season, args.end_season)


# --------------------------------------------------------------------------- #
# Extract: download release files into the landing volume
# --------------------------------------------------------------------------- #


def build_http_session() -> requests.Session:
    """HTTP session that retries throttling and transient server errors with backoff."""
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "GridironLakehouse-bronze-ingest"
    return session


def assert_complete_parquet(path: str, expected_bytes: int | None) -> None:
    """Raise if ``path`` is truncated or is not a Parquet file.

    A Parquet file starts and ends with the magic bytes ``PAR1``. The footer is
    written last, so a download that was cut short fails this check.
    """
    actual_bytes = os.path.getsize(path)
    if expected_bytes is not None and actual_bytes != expected_bytes:
        raise IOError(f"{path}: expected {expected_bytes:,} bytes, received {actual_bytes:,}")
    if actual_bytes < 2 * len(PARQUET_MAGIC):
        raise IOError(f"{path}: {actual_bytes} bytes is too small to be a Parquet file")

    with open(path, "rb") as handle:
        head = handle.read(len(PARQUET_MAGIC))
        handle.seek(-len(PARQUET_MAGIC), os.SEEK_END)
        tail = handle.read(len(PARQUET_MAGIC))
    if head != PARQUET_MAGIC or tail != PARQUET_MAGIC:
        raise IOError(f"{path}: not a complete Parquet file")


def download_to_landing(session: requests.Session, url: str, landing_path: str) -> bool:
    """Download ``url`` to ``landing_path``. Return False if the file does not exist.

    The file is written to driver-local disk and copied into the volume only once
    it has passed ``assert_complete_parquet``, so a dropped connection can never
    leave a truncated file where Spark will read it.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = os.path.join(tmp_dir, os.path.basename(landing_path))

        with session.get(url, stream=True, timeout=HTTP_TIMEOUT_SECONDS) as response:
            if response.status_code == 404:
                return False
            response.raise_for_status()

            with open(tmp_path, "wb") as handle:
                for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                    handle.write(chunk)

            # Content-Length is the size on the wire, so it is only comparable
            # when the server did not compress the response.
            content_length = response.headers.get("Content-Length")
            is_compressed = "Content-Encoding" in response.headers
            expected_bytes = int(content_length) if content_length and not is_compressed else None

        assert_complete_parquet(tmp_path, expected_bytes)
        os.makedirs(os.path.dirname(landing_path), exist_ok=True)
        shutil.copyfile(tmp_path, landing_path)

    log.info("Landed %s (%.1f MB)", landing_path, os.path.getsize(landing_path) / 1e6)
    return True


def land_play_by_play(session: requests.Session, config: IngestConfig) -> dict[int, str]:
    """Download every requested season. Return ``{season: landed file path}``."""
    landed: dict[int, str] = {}
    for season in config.seasons:
        url = PBP_URL.format(season=season)
        path = f"{config.landing_root}/pbp/play_by_play_{season}.parquet"

        if download_to_landing(session, url, path):
            landed[season] = path
        elif season == config.end_season:
            # nflverse publishes a season's file once its first games are played.
            log.warning("Season %s is not published yet, skipping (%s)", season, url)
        else:
            raise FileNotFoundError(f"nflverse has no play-by-play file for season {season}: {url}")
    return landed


# --------------------------------------------------------------------------- #
# Transform: schema reconciliation and audit columns (no business logic)
# --------------------------------------------------------------------------- #


def widest_type(types: list[T.DataType]) -> T.DataType:
    """Return a type that every type in ``types`` can be cast to without failing.

    Integers widen to the largest integer seen, mixed numerics widen to DOUBLE,
    and anything else falls back to STRING.
    """
    distinct = list({dtype.simpleString(): dtype for dtype in types}.values())
    if len(distinct) == 1:
        return distinct[0]
    if all(isinstance(dtype, INTEGRAL_TYPE_ORDER) for dtype in distinct):
        return max(distinct, key=lambda dtype: INTEGRAL_TYPE_ORDER.index(type(dtype)))
    if all(isinstance(dtype, T.NumericType) for dtype in distinct):
        return T.DoubleType()
    return T.StringType()


def align_to_common_schema(
    frames: list[DataFrame], table_schema: T.StructType | None = None
) -> list[DataFrame]:
    """Cast every frame to one shared schema so the frames can be unioned.

    A column's type is the widest one seen across the frames and, when the Delta
    table already exists, in the table. Including the table means a single-season
    refresh is widened to match what earlier loads established. If nflverse ever
    ships a type wider than the table's, the frames keep it and Delta rejects the
    write, which is the loud failure we want instead of a silent narrowing cast.

    A column missing from a frame is filled with NULLs.
    """
    seen: dict[str, list[T.DataType]] = {}
    for frame in frames:
        for field in frame.schema.fields:
            seen.setdefault(field.name, []).append(field.dataType)

    # Table columns the incoming files no longer carry are left alone: Delta
    # writes NULL for them.
    for field in table_schema.fields if table_schema else []:
        if field.name in seen:
            seen[field.name].append(field.dataType)

    common = {name: widest_type(types) for name, types in seen.items()}

    aligned = []
    for frame in frames:
        present = set(frame.columns)
        aligned.append(
            frame.select(
                [
                    (F.col(name) if name in present else F.lit(None)).cast(dtype).alias(name)
                    for name, dtype in common.items()
                ]
            )
        )
    return aligned


def with_audit_columns(frame: DataFrame, run_id: str) -> DataFrame:
    """Add lineage columns. ``_ingested_at`` is the watermark dbt loads incrementally from."""
    return frame.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_ingest_run_id", F.lit(run_id)
    )


def read_landed_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Read one landed file. The schema is inferred from the Parquet footer."""
    # _metadata is only resolvable directly on the file scan, so capture it here.
    return spark.read.parquet(path).withColumn("_source_file", F.col("_metadata.file_path"))


def build_play_by_play(
    spark: SparkSession, paths: list[str], table_schema: T.StructType | None, run_id: str
) -> DataFrame:
    """Read the landed season files into one frame with one schema and audit columns."""
    frames = [read_landed_parquet(spark, path) for path in paths]
    frames = align_to_common_schema(frames, table_schema)
    # Called on the instance, not as DataFrame.unionByName, so it works the same
    # on classic clusters and on Spark Connect (serverless, shared access mode).
    plays = reduce(lambda left, right: left.unionByName(right), frames)
    return with_audit_columns(plays, run_id)


# --------------------------------------------------------------------------- #
# Load: write Delta tables
# --------------------------------------------------------------------------- #


def ensure_namespace(spark: SparkSession, config: IngestConfig) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{config.catalog}`.`{config.schema}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {config.qualified(LANDING_VOLUME)}")


def write_play_by_play(spark: SparkSession, frame: DataFrame, table: str, seasons: list[int]) -> None:
    """Create the table on the first run, otherwise replace only ``seasons``."""
    writer = (
        # One shuffle partition per season, so each season is written as one
        # compact file instead of a fragment per task.
        frame.repartition("season")
        .write.format("delta")
        .partitionBy("season")
        .mode("overwrite")
    )

    if spark.catalog.tableExists(table):
        season_list = ", ".join(str(season) for season in sorted(seasons))
        (
            # Atomically swaps the listed partitions and leaves the others alone.
            # Delta also fails the write if a row falls outside the predicate.
            writer.option("replaceWhere", f"season IN ({season_list})")
            # Accept columns nflverse adds over time.
            .option("mergeSchema", "true")
            .saveAsTable(table)
        )
        return

    writer.saveAsTable(table)
    spark.sql(
        f"""
        ALTER TABLE {table} SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
    )
    spark.sql(
        f"COMMENT ON TABLE {table} IS "
        "'Raw nflverse play-by-play, one row per play. Loaded by bronze_ingest_nflverse.py.'"
    )


def validate_play_by_play(spark: SparkSession, table: str, run_id: str, seasons: list[int]) -> None:
    """Fail the run if any season it was meant to load has no rows in the table."""
    rows = (
        spark.table(table)
        .where(F.col("_ingest_run_id") == run_id)
        .groupBy("season")
        .count()
        .collect()
    )
    counts = {row["season"]: row["count"] for row in rows}

    missing = sorted(set(seasons) - set(counts))
    if missing:
        raise RuntimeError(f"{table}: no rows were written for seasons {missing}")
    for season in sorted(counts):
        log.info("%s season %s: %s plays", table, season, f"{counts[season]:,}")


def ingest_play_by_play(
    spark: SparkSession, session: requests.Session, config: IngestConfig, run_id: str
) -> None:
    table = config.qualified(PBP_TABLE)

    landed = land_play_by_play(session, config)
    if not landed:
        log.warning("No play-by-play files were available, %s is unchanged", table)
        return

    table_schema = spark.table(table).schema if spark.catalog.tableExists(table) else None
    plays = build_play_by_play(spark, list(landed.values()), table_schema, run_id)

    write_play_by_play(spark, plays, table, list(landed))
    validate_play_by_play(spark, table, run_id, list(landed))


def ingest_players(
    spark: SparkSession, session: requests.Session, config: IngestConfig, run_id: str
) -> None:
    """Full snapshot of the player master. It is small, so it is replaced every run."""
    table = config.qualified(PLAYERS_TABLE)
    path = f"{config.landing_root}/players/players.parquet"

    if not download_to_landing(session, PLAYERS_URL, path):
        raise FileNotFoundError(f"nflverse players file not found: {PLAYERS_URL}")

    players = with_audit_columns(read_landed_parquet(spark, path), run_id)
    (
        players.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    log.info("%s: %s players", table, f"{spark.table(table).count():,}")


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> None:
    config = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    session = build_http_session()
    run_id = str(uuid.uuid4())

    log.info(
        "Run %s: seasons %s-%s into %s.%s",
        run_id, config.start_season, config.end_season, config.catalog, config.schema,
    )
    ensure_namespace(spark, config)
    ingest_players(spark, session, config, run_id)
    ingest_play_by_play(spark, session, config, run_id)
    log.info("Run %s finished", run_id)


if __name__ == "__main__":
    main()
