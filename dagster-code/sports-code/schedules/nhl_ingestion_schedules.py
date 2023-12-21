from dagster import build_schedule_from_partitioned_job
from jobs.nhl_ingestion_jobs import nhl_schedule_job

daily_nhl_schedule = build_schedule_from_partitioned_job(
    nhl_schedule_job,
    description="loads the daily partition for nhl schedule",
    hour_of_day=3
)
