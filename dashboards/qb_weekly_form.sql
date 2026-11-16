-- Dataset: qb_weekly_form
-- One row per week for the six leading passers of the latest regular season.
-- Feeds the passing-yards race and the rolling 4-game line charts.

with season_games as (

    select *
    from gridiron.gold.gold_qb_efficiency
    where season_type = 'REG'
        and season = (select max(season) from gridiron.gold.gold_qb_efficiency)

),

yardage_leaders as (

    -- Six lines is about as many as a line chart can carry.
    select player_id
    from season_games
    where is_latest_game
    order by season_passing_yards desc
    limit 6

)

select
    season_games.week,
    season_games.player_name,
    season_games.passing_yards as game_passing_yards,
    season_games.season_passing_yards,
    -- A rolling value is only shown once its 4-game window is full. Before
    -- that it is an average over one to three games and swings wildly.
    case
        when season_games.rolling_games_played >= 4
            then round(season_games.rolling_completion_pct, 1)
    end as rolling_completion_pct,
    case
        when season_games.rolling_games_played >= 4
            then round(season_games.rolling_late_down_epa_per_play, 3)
    end as rolling_late_down_epa_per_play
from season_games
inner join yardage_leaders
    on season_games.player_id = yardage_leaders.player_id
order by season_games.week, season_games.player_name
