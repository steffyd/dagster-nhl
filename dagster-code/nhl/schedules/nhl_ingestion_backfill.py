from dagster import build_schedule_from_partitioned_job
from jobs.nhl_ingestion_jobs import daily_nhl_games_job, daily_nhl_schedule_job


daily_team_stats_backfill = build_schedule_from_partitioned_job(
    daily_nhl_games_job,
    description="loads the daily partition for nhl games",
    hour_of_day=9,
)
daily_schedule_backfill = build_schedule_from_partitioned_job(
    daily_nhl_schedule_job,
    description="loads the daily partition for nhl schedule",
    hour_of_day=9,
)
