from .nhl_ingestion_backfill import daily_team_stats_backfill, daily_nhl_schedule

SCHEDULES = [daily_team_stats_backfill, daily_nhl_schedule]
