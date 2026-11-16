-- Grain: one row per player (Type 1: attributes hold their latest known value).
--
-- Names and positions come from the nflverse player master. Play-by-play can
-- reference a player before the master lists them, typically a debut in a game
-- that was loaded before the master was refreshed. Such a player gets an
-- "inferred member" row built from what the plays say, so every player key in
-- fact_plays always resolves. The row is replaced by the full record on the
-- first run after the master catches up.

with player_master as (

    select * from {{ ref('stg_nflverse__players') }}

),

plays as (

    select * from {{ ref('stg_nflverse__plays') }}

),

-- Every player reference fact_plays carries, one row per sighting.
play_participants as (

    select dropback_player_id as player_id, dropback_player_name as player_name from plays
    union all
    select passer_player_id, passer_player_name from plays
    union all
    select rusher_player_id, rusher_player_name from plays
    union all
    select receiver_player_id, receiver_player_name from plays

),

observed_players as (

    select
        player_id,
        -- Play-by-play only has the short form ("P.Mahomes"), and its spelling
        -- can vary between games. max() picks one deterministically.
        max(player_name) as player_name

    from play_participants
    where player_id is not null
    group by player_id

),

inferred_members as (

    select
        observed_players.player_id,
        observed_players.player_name

    from observed_players
    left join player_master
        on observed_players.player_id = player_master.player_id
    where player_master.player_id is null

),

final as (

    select
        player_id,
        player_name,
        first_name,
        last_name,
        position,
        position_group,
        height_inches,
        weight_lbs,
        birth_date,
        college_name,
        rookie_season,
        last_season,
        latest_team,
        roster_status,
        draft_year,
        draft_round,
        draft_pick,
        false as is_inferred_member

    from player_master

    union all

    select
        player_id,
        coalesce(player_name, 'Unknown') as player_name,
        cast(null as string) as first_name,
        cast(null as string) as last_name,
        'UNK' as position,
        'UNK' as position_group,
        cast(null as int) as height_inches,
        cast(null as int) as weight_lbs,
        cast(null as date) as birth_date,
        cast(null as string) as college_name,
        cast(null as int) as rookie_season,
        cast(null as int) as last_season,
        cast(null as string) as latest_team,
        cast(null as string) as roster_status,
        cast(null as int) as draft_year,
        cast(null as int) as draft_round,
        cast(null as int) as draft_pick,
        true as is_inferred_member

    from inferred_members

)

select * from final
