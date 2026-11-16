# Dashboard datasets

SQL for the Databricks AI/BI dashboard. Each file is one dataset. They read the
production schemas (`gridiron.gold`, `gridiron.silver`), so run
`dbt build --target prod` first.

The quarterback datasets always select the latest season in the data, so the
dashboard needs no filter and follows the weekly refresh on its own.

## Recreating the dashboard

1. In Databricks, create a dashboard.
2. On the **Data** tab, create one dataset per file: paste the SQL and name the
   dataset after the file.
3. On the **Canvas** tab, add the widgets below.

| Widget | Dataset | Chart | Fields |
|---|---|---|---|
| Plays, games, seasons, latest game | `pipeline_summary` | counters | one column each |
| Season leaderboard | `qb_season_leaders` | table | all columns |
| EPA per play on 3rd and 4th down | `qb_season_leaders` | horizontal bar | Y `player_name`, X `late_down_epa_per_play` |
| Accuracy vs late-down efficiency | `qb_season_leaders` | scatter | X `completion_pct`, Y `late_down_epa_per_play`, size `dropbacks` |
| Passing yards race | `qb_weekly_form` | line | X `week`, Y `season_passing_yards`, color `player_name` |
| Rolling 4-game late-down EPA | `qb_weekly_form` | line | X `week`, Y `rolling_late_down_epa_per_play`, color `player_name` |
| Passing by game conditions | `passing_by_conditions` | bar | X `conditions`, Y `epa_per_dropback` |
| Play mix | `plays_by_type` | bar | X `play_type`, Y `play_count` |

Every chart point is a single row of its dataset, so the aggregation a chart
asks for does not change the value: use SUM for yards and counts, AVG for rates.

## Notes on the definitions

- **Qualified quarterbacks.** `qb_season_leaders` keeps quarterbacks with at
  least 14 dropbacks per game played so far, so the cutoff scales with the
  season instead of being a fixed number.
- **Rolling values.** `qb_weekly_form` shows a rolling metric only once its
  4-game window is full. Before that it is an average over one to three games.
- **Game conditions.** Outdoor games with no recorded temperature are left out
  of `passing_by_conditions`.
