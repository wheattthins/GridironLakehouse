-- Grain: one row per play, including rows that are not snaps (kickoffs,
-- timeouts, end of quarter). Filter on play_type or the is_* flags as needed.
--
-- Loaded incrementally: each run merges the plays written by ingestion runs it
-- has not seen yet. A season that nflverse republishes arrives in bronze with a
-- new _ingested_at and is upserted on play_key.

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='play_key',
        partition_by='season',
        on_schema_change='append_new_columns'
    )
}}

with plays as (

    select * from {{ ref('stg_nflverse__plays') }}

    {% if is_incremental() %}
    where _ingested_at > (
        select coalesce(max(_ingested_at), cast('1900-01-01' as timestamp)) from {{ this }}
    )
    {% endif %}

),

final as (

    select
        -- primary key: play_id alone is only unique within a game
        {{ dbt_utils.generate_surrogate_key(['game_id', 'play_id']) }} as play_key,

        -- foreign key to dim_games, and the play's id within that game
        game_id,
        play_id,

        -- foreign keys to dim_players, one per role. NULL when the role does
        -- not apply to the play.
        dropback_player_id,
        passer_player_id,
        rusher_player_id,
        receiver_player_id,

        -- degenerate dimensions
        season,
        possession_team,
        defense_team,
        play_type,
        play_description,

        -- situation before the snap
        quarter,
        down,
        yards_to_go,
        yardline_100,
        is_goal_to_go,
        game_seconds_remaining,
        half_seconds_remaining,
        score_differential,
        possession_team_score,
        defense_team_score,
        is_shotgun,
        is_no_huddle,

        -- outcome flags
        is_qb_dropback,
        is_qb_scramble,
        -- An official pass attempt, spikes included. The source pass_attempt
        -- flag also counts sacks, which would deflate completion percentage.
        (is_complete_pass or is_incomplete_pass or is_interception) as is_pass_attempt,
        is_complete_pass,
        is_interception,
        is_sack,
        is_rush_attempt,
        is_touchdown,
        is_pass_touchdown,
        is_rush_touchdown,
        is_first_down,
        is_fumble_lost,
        is_penalty,
        is_two_point_attempt,
        is_successful_play,

        -- additive measures
        yards_gained,
        passing_yards,
        rushing_yards,
        receiving_yards,
        air_yards,
        yards_after_catch,
        epa,
        wpa,

        -- non-additive measures: a state before the snap, not a change
        expected_points,
        win_probability,

        -- audit
        _ingested_at

    from plays

)

select * from final
