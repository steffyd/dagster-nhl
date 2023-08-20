from dagster import build_schedule_from_partitioned_job
from jobs.nhl_ingestion_jobs import nhl_games_job, nhl_schedule_job


daily_nhl_games_schedule = build_schedule_from_partitioned_job(
    nhl_games_job,
    description="loads the daily partition for nhl games",
    hour_of_day=3,
)
daily_nhl_schedule = build_schedule_from_partitioned_job(
    nhl_schedule_job,
    description="loads the daily partition for nhl schedule",
    hour_of_day=3
)
