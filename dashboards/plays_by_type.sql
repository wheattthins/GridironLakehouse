-- Dataset: plays_by_type
-- Number of plays of each type across every season. Feeds the play mix bar chart.

select
    play_type,
    count(*) as play_count
from gridiron.silver.fact_plays
-- Rows without a play type are not plays (timeouts, end of quarter, ...).
where play_type is not null
group by play_type
order by play_type
