-- Reconciliation: fact_plays must hold exactly the plays bronze holds, season
-- by season. Returns one row per season that is out of balance.
--
-- fact_plays is merged on play_key, and a merge never deletes. If nflverse
-- republishes a season with a play removed or renumbered, the old row would
-- linger in the fact table. This test is what catches that. The fix is a
-- rebuild from bronze:
--
--     dbt build --full-refresh --select fact_plays+

with bronze as (

    select
        season,
        count(*) as play_count
    from {{ source('nflverse', 'bronze_nfl_pbp') }}
    group by season

),

fact as (

    select
        season,
        count(*) as play_count
    from {{ ref('fact_plays') }}
    group by season

)

select
    coalesce(bronze.season, fact.season) as season,
    bronze.play_count as bronze_play_count,
    fact.play_count as fact_play_count

from bronze
full outer join fact
    on bronze.season = fact.season
where coalesce(bronze.play_count, 0) <> coalesce(fact.play_count, 0)
