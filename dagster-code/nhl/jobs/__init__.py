from .nhl_ingestion_jobs import daily_nhl_games_job, daily_nhl_schedule_job

JOBS = [daily_nhl_games_job, daily_nhl_schedule_job]