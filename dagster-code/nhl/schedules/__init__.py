from .nhl_ingestion_backfill import daily_team_stats_backfill, daily_schedule_backfill

SCHEDULES = [daily_team_stats_backfill, daily_schedule_backfill]
