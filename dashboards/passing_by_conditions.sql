-- Dataset: passing_by_conditions
-- Passing efficiency by roof and temperature across every season. Joins
-- fact_plays to dim_games, so it shows the star schema being used.

with dropbacks as (

    select
        -- The number prefix makes the chart sort the buckets from indoors to
        -- warm instead of alphabetically.
        case
            when games.roof in ('dome', 'closed') then '1. Indoors'
            when games.temperature_f < 32 then '2. Below 32F'
            when games.temperature_f < 50 then '3. 32 to 49F'
            when games.temperature_f < 70 then '4. 50 to 69F'
            else '5. 70F and above'
        end as conditions,
        plays.epa,
        plays.is_pass_attempt,
        plays.is_complete_pass
    from gridiron.silver.fact_plays as plays
    inner join gridiron.silver.dim_games as games
        on plays.game_id = games.game_id
    where plays.is_qb_dropback
        and not plays.is_two_point_attempt
        -- Outdoor games without a recorded temperature cannot be bucketed.
        and (games.roof in ('dome', 'closed') or games.temperature_f is not null)

)

select
    conditions,
    count(*) as dropbacks,
    round(avg(epa), 3) as epa_per_dropback,
    round(100.0 * count_if(is_complete_pass) / nullif(count_if(is_pass_attempt), 0), 1) as completion_pct
from dropbacks
group by conditions
order by conditions
