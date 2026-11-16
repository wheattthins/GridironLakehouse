-- One row per player in the nflverse player master. Renames and casts only.

with source as (

    select * from {{ source('nflverse', 'bronze_nfl_players') }}

),

renamed as (

    select
        gsis_id as player_id,
        display_name as player_name,
        first_name,
        last_name,
        position,
        position_group,
        height as height_inches,
        weight as weight_lbs,
        -- A malformed birth date should not fail the build, hence try_cast.
        try_cast(birth_date as date) as birth_date,
        college_name,
        rookie_season,
        last_season,
        latest_team,
        status as roster_status,
        draft_year,
        draft_round,
        draft_pick,
        _ingested_at

    from source

    -- A player without a GSIS id cannot be joined to play-by-play.
    where gsis_id is not null

)

select * from renamed
