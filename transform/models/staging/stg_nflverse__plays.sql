-- One row per play. Selects the columns the warehouse uses out of the 372 in
-- bronze, then renames and casts them. No joins, filters or business logic.

with source as (

    select * from {{ source('nflverse', 'bronze_nfl_pbp') }}

),

renamed as (

    select
        -- keys
        game_id,
        cast(play_id as int) as play_id,

        -- players
        -- passer_id is the quarterback on every dropback, scrambles included.
        -- passer_player_id is only set when a pass was thrown or a sack taken.
        passer_id as dropback_player_id,
        passer as dropback_player_name,
        passer_player_id,
        passer_player_name,
        rusher_player_id,
        rusher_player_name,
        receiver_player_id,
        receiver_player_name,

        -- game attributes, repeated on every play of a game
        season,
        season_type,
        week,
        cast(game_date as date) as game_date,
        home_team,
        away_team,
        home_score,
        away_score,
        home_coach,
        away_coach,
        coalesce(location = 'Neutral', false) as is_neutral_site,
        {{ flag_to_boolean('div_game') }} as is_division_game,
        stadium_id,
        stadium as stadium_name,
        roof,
        nullif(trim(surface), '') as surface,
        temp as temperature_f,
        wind as wind_mph,
        weather as weather_description,

        -- situation before the snap
        posteam as possession_team,
        defteam as defense_team,
        cast(qtr as int) as quarter,
        cast(down as int) as down,
        cast(ydstogo as int) as yards_to_go,
        cast(yardline_100 as int) as yardline_100,
        cast(game_seconds_remaining as int) as game_seconds_remaining,
        cast(half_seconds_remaining as int) as half_seconds_remaining,
        cast(score_differential as int) as score_differential,
        cast(posteam_score as int) as possession_team_score,
        cast(defteam_score as int) as defense_team_score,
        {{ flag_to_boolean('goal_to_go') }} as is_goal_to_go,
        {{ flag_to_boolean('shotgun') }} as is_shotgun,
        {{ flag_to_boolean('no_huddle') }} as is_no_huddle,

        -- what happened
        play_type,
        {{ adapter.quote('desc') }} as play_description,
        {{ flag_to_boolean('qb_dropback') }} as is_qb_dropback,
        {{ flag_to_boolean('qb_scramble') }} as is_qb_scramble,
        {{ flag_to_boolean('complete_pass') }} as is_complete_pass,
        {{ flag_to_boolean('incomplete_pass') }} as is_incomplete_pass,
        {{ flag_to_boolean('interception') }} as is_interception,
        {{ flag_to_boolean('sack') }} as is_sack,
        {{ flag_to_boolean('rush_attempt') }} as is_rush_attempt,
        {{ flag_to_boolean('touchdown') }} as is_touchdown,
        {{ flag_to_boolean('pass_touchdown') }} as is_pass_touchdown,
        {{ flag_to_boolean('rush_touchdown') }} as is_rush_touchdown,
        {{ flag_to_boolean('first_down') }} as is_first_down,
        {{ flag_to_boolean('fumble_lost') }} as is_fumble_lost,
        {{ flag_to_boolean('penalty') }} as is_penalty,
        {{ flag_to_boolean('two_point_attempt') }} as is_two_point_attempt,
        {{ flag_to_boolean('success') }} as is_successful_play,

        -- measures
        cast(yards_gained as int) as yards_gained,
        cast(passing_yards as int) as passing_yards,
        cast(rushing_yards as int) as rushing_yards,
        cast(receiving_yards as int) as receiving_yards,
        cast(air_yards as int) as air_yards,
        cast(yards_after_catch as int) as yards_after_catch,
        ep as expected_points,
        epa,
        wp as win_probability,
        wpa,

        -- audit
        _ingested_at

    from source

)

select * from renamed
