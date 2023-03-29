from dagster import DailyPartitionsDefinition


nhl_daily_partition = DailyPartitionsDefinition(start_date="1990-08-01")
nhl_future_week_daily_partition = DailyPartitionsDefinition(start_date="1990-08-01", end_offset= 7)