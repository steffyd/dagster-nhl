from dagster import DailyPartitionsDefinition


nhl_daily_partition = DailyPartitionsDefinition(start_date="1990-08-01", timezone=f"US/Pacific")
nhl_future_week_daily_partition = DailyPartitionsDefinition(
    start_date="1990-08-01",
    end_offset=7,
    timezone=f"US/Pacific"
)
