-- Dataset: qb_season_leaders
-- One row per qualified quarterback for the latest regular season, ranked by
-- EPA per play on 3rd and 4th down. Feeds the leaderboard table, the ranked
-- bar chart and the accuracy-versus-efficiency scatter plot.

with season_totals as (

    -- One row per quarterback: the latest game of the current regular season
    -- carries the season-to-date totals.
    select *
    from gridiron.gold.gold_qb_efficiency
    where season_type = 'REG'
        and is_latest_game
        and season = (select max(season) from gridiron.gold.gold_qb_efficiency)

),

qualified as (

    -- Rate statistics need a minimum sample: 14 dropbacks per game played so
    -- far, so the cutoff grows with the season instead of being hard-coded.
    select *
    from season_totals
    where season_dropbacks >= 14 * (select max(season_games_played) from season_totals)

)

select
    rank() over (order by season_late_down_epa_per_play desc) as late_down_rank,
    player_name,
    team,
    season,
    season_games_played as games,
    season_dropbacks as dropbacks,
    season_passing_yards as passing_yards,
    round(season_completion_pct, 1) as completion_pct,
    season_passing_touchdowns as touchdowns,
    season_interceptions as interceptions,
    round(season_td_int_ratio, 2) as td_int_ratio,
    season_late_down_dropbacks as late_down_dropbacks,
    round(season_late_down_epa_per_play, 3) as late_down_epa_per_play
from qualified
order by late_down_rank
