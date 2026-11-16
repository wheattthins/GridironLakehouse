-- How well does a quarterback's season predict the next one?
--
-- Pairs each quarterback season with the same quarterback's following season
-- and correlates the two, once for completion percentage and once for EPA per
-- play on 3rd and 4th down. A higher correlation means the metric is a more
-- stable trait of the player and less a product of one season's circumstances.

with season_totals as (

    select
        player_id,
        season,
        season_completion_pct,
        season_late_down_epa_per_play
    from gridiron.gold.gold_qb_efficiency
    where season_type = 'REG'
        and is_latest_game
        -- Roughly a full season as a starter.
        and season_dropbacks >= 200
        -- The latest season may still be in progress.
        and season < (select max(season) from gridiron.gold.gold_qb_efficiency)

),

consecutive_seasons as (

    select
        this_season.season_completion_pct as completion_pct,
        next_season.season_completion_pct as next_completion_pct,
        this_season.season_late_down_epa_per_play as late_down_epa,
        next_season.season_late_down_epa_per_play as next_late_down_epa
    from season_totals as this_season
    inner join season_totals as next_season
        on this_season.player_id = next_season.player_id
        and next_season.season = this_season.season + 1

)

select
    count(*) as quarterback_season_pairs,
    round(corr(completion_pct, next_completion_pct), 2) as completion_pct_correlation,
    round(corr(late_down_epa, next_late_down_epa), 2) as late_down_epa_correlation
from consecutive_seasons
