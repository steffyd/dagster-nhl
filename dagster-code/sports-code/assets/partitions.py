from dagster import DailyPartitionsDefinition, WeeklyPartitionsDefinition


nhl_weekly_partition = WeeklyPartitionsDefinition(start_date="1990-09-04", timezone=f"US/Pacific")