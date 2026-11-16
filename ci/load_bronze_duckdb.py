"""
GridironLakehouse | DuckDB stand-in for the bronze layer
========================================================

CI and local development run the dbt project on DuckDB instead of a Databricks
SQL Warehouse. This script builds the two tables the dbt sources read, with the
same names and audit columns the Databricks job
(ingestion/bronze_ingest_nflverse.py) produces:

    bronze.bronze_nfl_pbp
    bronze.bronze_nfl_players

Each run replaces both tables and stamps a new ``_ingested_at``. To the
incremental fact model that is exactly what a refreshed load looks like, so
running this script and ``dbt build`` twice exercises the merge path.

Usage
-----
    export GRIDIRON_DUCKDB_PATH="$PWD/gridiron.duckdb"
    python ci/load_bronze_duckdb.py --seasons 2023 2024
"""

from __future__ import annotations

import argparse
import os
import tempfile
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

NFLVERSE_RELEASES = "https://github.com/nflverse/nflverse-data/releases/download"
PBP_URL = NFLVERSE_RELEASES + "/pbp/play_by_play_{season}.parquet"
PLAYERS_URL = NFLVERSE_RELEASES + "/players/players.parquet"

# nflverse stores goal_to_go as INT32 in 2023 and DOUBLE in 2024, so the default
# sample contains the same schema drift the production job has to reconcile.
DEFAULT_SEASONS = [2023, 2024]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load a sample of nflverse data into DuckDB.")
    parser.add_argument("--seasons", type=int, nargs="+", default=DEFAULT_SEASONS)
    parser.add_argument(
        "--database",
        default=os.environ.get("GRIDIRON_DUCKDB_PATH"),
        help="DuckDB file to write (default: $GRIDIRON_DUCKDB_PATH)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path(tempfile.gettempdir()) / "gridiron_nflverse",
        help="Where downloaded files are kept between runs",
    )
    args = parser.parse_args()
    if not args.database:
        parser.error("pass --database or set GRIDIRON_DUCKDB_PATH")
    return args


def download(url: str, cache_dir: Path) -> Path:
    """Download ``url`` into ``cache_dir`` unless it is already there."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    target = cache_dir / url.rsplit("/", 1)[-1]
    if not target.exists():
        # Write under another name first so an interrupted download is never
        # mistaken for a complete file on the next run.
        partial = target.with_name(target.name + ".part")
        urllib.request.urlretrieve(url, partial)
        partial.replace(target)
    return target


def sql_string(path: Path) -> str:
    """``path`` as a SQL string literal."""
    return "'" + path.as_posix().replace("'", "''") + "'"


def main() -> None:
    args = parse_args()
    run_id = str(uuid.uuid4())
    # One timestamp for the whole run, as in the Databricks job.
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    audit_columns = f"timestamp '{ingested_at}' as _ingested_at, '{run_id}' as _ingest_run_id"

    pbp_files = [download(PBP_URL.format(season=season), args.cache_dir) for season in args.seasons]
    players_file = download(PLAYERS_URL, args.cache_dir)

    connection = duckdb.connect(args.database)
    connection.execute("create schema if not exists bronze")

    # union_by_name aligns the seasons by column name and widens conflicting
    # types, which is what align_to_common_schema does in the PySpark job.
    pbp_file_list = ", ".join(sql_string(path) for path in pbp_files)
    connection.execute(
        f"""
        create or replace table bronze.bronze_nfl_pbp as
        select * exclude (filename), filename as _source_file, {audit_columns}
        from read_parquet([{pbp_file_list}], union_by_name = true, filename = true)
        """
    )
    connection.execute(
        f"""
        create or replace table bronze.bronze_nfl_players as
        select * exclude (filename), filename as _source_file, {audit_columns}
        from read_parquet({sql_string(players_file)}, filename = true)
        """
    )

    plays_by_season = connection.execute(
        "select season, count(*) from bronze.bronze_nfl_pbp group by season order by season"
    ).fetchall()
    players = connection.execute("select count(*) from bronze.bronze_nfl_players").fetchall()[0][0]
    for season, plays in plays_by_season:
        print(f"bronze_nfl_pbp season {season}: {plays:,} plays")
    print(f"bronze_nfl_players: {players:,} players")
    print(f"written to {args.database} at {ingested_at} UTC")


if __name__ == "__main__":
    main()
