-- What would completion percentage be if the source flag were trusted?
--
-- nflverse sets pass_attempt on sacks as well as on passes. This compares the
-- league completion rate computed from that flag with the official definition
-- (completions + incompletions + interceptions), which is what fact_plays
-- derives as is_pass_attempt.

with pass_plays as (

    select
        complete_pass,
        pass_attempt as raw_flag,
        case
            when complete_pass = 1 or incomplete_pass = 1 or interception = 1 then 1
            else 0
        end as official_attempt,
        sack
    from gridiron.bronze.bronze_nfl_pbp
    where two_point_attempt = 0

)

select
    sum(complete_pass) as completions,
    sum(raw_flag) as raw_flag_attempts,
    sum(official_attempt) as official_attempts,
    sum(case when raw_flag = 1 and sack = 1 then 1 else 0 end) as sacks_flagged_as_attempts,
    round(100.0 * sum(complete_pass) / sum(raw_flag), 1) as completion_pct_from_raw_flag,
    round(100.0 * sum(complete_pass) / sum(official_attempt), 1) as completion_pct_official
from pass_plays
