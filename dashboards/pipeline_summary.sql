-- Dataset: pipeline_summary
-- One row of headline numbers for the counter widgets.

select
    (select count(*) from gridiron.silver.fact_plays) as plays,
    (select count(*) from gridiron.silver.dim_games) as games,
    (select count(distinct season) from gridiron.silver.dim_games) as seasons,
    (select count(distinct player_id) from gridiron.gold.gold_qb_efficiency) as quarterbacks,
    (select max(game_date) from gridiron.silver.dim_games) as latest_game_date
