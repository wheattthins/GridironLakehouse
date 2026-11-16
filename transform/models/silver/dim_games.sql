-- Grain: one row per game.
--
-- nflverse repeats a game's attributes on every one of its plays. They are
-- constant within a game, so max() collapses them to a single row and the
-- grain holds by construction instead of depending on a "pick one row" rule.

with plays as (

    select * from {{ ref('stg_nflverse__plays') }}

),

games as (

    select
        game_id,
        max(season) as season,
        max(season_type) as season_type,
        max(week) as week,
        max(game_date) as game_date,
        max(home_team) as home_team,
        max(away_team) as away_team,
        max(home_score) as home_score,
        max(away_score) as away_score,
        max(home_coach) as home_coach,
        max(away_coach) as away_coach,
        bool_or(is_division_game) as is_division_game,
        bool_or(is_neutral_site) as is_neutral_site,
        max(stadium_id) as stadium_id,
        max(stadium_name) as stadium_name,
        max(roof) as roof,
        max(surface) as surface,
        max(temperature_f) as temperature_f,
        max(wind_mph) as wind_mph,
        max(weather_description) as weather_description

    from plays
    group by game_id

),

final as (

    select
        -- key
        game_id,

        -- schedule
        season,
        season_type,
        week,
        game_date,

        -- teams and result
        home_team,
        away_team,
        home_score,
        away_score,
        -- NULL for a tie
        case
            when home_score > away_score then home_team
            when away_score > home_score then away_team
        end as winning_team,
        home_coach,
        away_coach,
        is_division_game,

        -- venue
        stadium_id,
        stadium_name,
        is_neutral_site,
        roof,
        surface,

        -- weather: temperature and wind are not recorded for most indoor games
        temperature_f,
        wind_mph,
        weather_description

    from games

)

select * from final
