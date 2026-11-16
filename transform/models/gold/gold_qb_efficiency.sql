-- Grain: one row per quarterback per game.
--
-- Each row holds the quarterback's line for that game and two sets of
-- point-in-time features derived from it:
--
--   season_*   season to date, through this game. The row with
--              is_latest_game = true carries the quarterback's season totals.
--   rolling_*  the last N games of the season, this one included
--              (N = var qb_rolling_window_games, default 4).
--
-- No window looks past the row's own game, so a row can be used as a feature
-- vector for anything that happens after that game without leaking the future.
-- The regular season and the playoffs accumulate separately.
--
-- Metric definitions
--   dropbacks               passes thrown + sacks + scrambles. Spikes are not dropbacks.
--   completion_pct          completions / pass attempts * 100. As in official
--                           statistics, a spike is an attempt and a sack is not.
--   td_int_ratio            passing touchdowns / interceptions. NULL while the
--                           quarterback has no interception in the window.
--   late_down_epa_per_play  mean EPA per dropback on 3rd and 4th down
-- Two-point attempts are excluded throughout, as in official statistics.

{% set window_size = var('qb_rolling_window_games') %}

{# Per-game values that can be summed across games. Every ratio below is
   computed from these sums, never by averaging per-game ratios. #}
{% set additive_measures = [
    'dropbacks',
    'pass_attempts',
    'completions',
    'passing_yards',
    'passing_touchdowns',
    'interceptions',
    'late_down_dropbacks',
    'late_down_epa',
] %}

with plays as (

    select * from {{ ref('fact_plays') }}

),

games as (

    select * from {{ ref('dim_games') }}

),

quarterbacks as (

    -- Keeps trick-play passes by receivers and punters out of the table.
    select * from {{ ref('dim_players') }}
    where position = 'QB'

),

qb_plays as (

    select
        -- dropback_player_id is empty on spikes, where only the passer is recorded.
        coalesce(dropback_player_id, passer_player_id) as player_id,
        game_id,
        possession_team,
        is_qb_dropback,
        is_pass_attempt,
        is_complete_pass,
        is_pass_touchdown,
        is_interception,
        passing_yards,
        -- EPA of a dropback on 3rd or 4th down, NULL for every other play.
        case when is_qb_dropback and down in (3, 4) then epa end as late_down_dropback_epa

    from plays
    where (is_qb_dropback or is_pass_attempt)
        and not is_two_point_attempt

),

qb_games as (

    select
        player_id,
        game_id,
        max(possession_team) as team,

        count_if(is_qb_dropback) as dropbacks,
        count_if(is_pass_attempt) as pass_attempts,
        count_if(is_complete_pass) as completions,
        coalesce(sum(passing_yards), 0) as passing_yards,
        count_if(is_pass_touchdown) as passing_touchdowns,
        count_if(is_interception) as interceptions,
        -- count(), not count(*): the denominator of the mean must only hold
        -- late-down dropbacks that have an EPA value.
        count(late_down_dropback_epa) as late_down_dropbacks,
        coalesce(sum(late_down_dropback_epa), 0) as late_down_epa

    from qb_plays
    group by player_id, game_id

),

accumulated as (

    select
        qb_games.player_id,
        quarterbacks.player_name,
        qb_games.team,
        games.season,
        games.season_type,
        games.week,
        qb_games.game_id,
        games.game_date,

        {% for measure in additive_measures -%}
        qb_games.{{ measure }},
        sum(qb_games.{{ measure }}) over season_to_date as season_{{ measure }},
        sum(qb_games.{{ measure }}) over trailing_games as rolling_{{ measure }},
        {% endfor -%}

        count(*) over season_to_date as season_games_played,
        -- Fewer than the window size until the quarterback has played that many games.
        count(*) over trailing_games as rolling_games_played,
        games.week = max(games.week) over (
            partition by qb_games.player_id, games.season, games.season_type
        ) as is_latest_game

    from qb_games
    inner join quarterbacks
        on qb_games.player_id = quarterbacks.player_id
    inner join games
        on qb_games.game_id = games.game_id

    window
        season_to_date as (
            partition by qb_games.player_id, games.season, games.season_type
            order by games.week
            rows between unbounded preceding and current row
        ),
        trailing_games as (
            partition by qb_games.player_id, games.season, games.season_type
            order by games.week
            rows between {{ window_size - 1 }} preceding and current row
        )

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['player_id', 'game_id']) }} as qb_game_key,
        player_id,
        player_name,
        team,
        season,
        season_type,
        week,
        game_id,
        game_date,
        is_latest_game,
        season_games_played,
        rolling_games_played,

        {#- The same metrics at three scopes: this game (no prefix), season to
            date and trailing games. One definition, so they cannot drift apart. #}
        {%- for scope in ['', 'season_', 'rolling_'] %}

        {{ scope }}dropbacks,
        {{ scope }}pass_attempts,
        {{ scope }}completions,
        {{ scope }}passing_yards,
        {{ scope }}passing_touchdowns,
        {{ scope }}interceptions,
        100 * {{ ratio(scope ~ 'completions', scope ~ 'pass_attempts') }} as {{ scope }}completion_pct,
        {{ ratio(scope ~ 'passing_touchdowns', scope ~ 'interceptions') }} as {{ scope }}td_int_ratio,
        {{ scope }}late_down_dropbacks,
        {{ ratio(scope ~ 'late_down_epa', scope ~ 'late_down_dropbacks') }} as {{ scope }}late_down_epa_per_play
        {%- if not loop.last %},{% endif %}
        {%- endfor %}

    from accumulated

)

select * from final
