from dagster import WeeklyPartitionsDefinition, DynamicPartitionsDefinition


nhl_weekly_partition = WeeklyPartitionsDefinition(start_date="1990-09-02", timezone=f"US/Pacific")
nhl_season_partition = DynamicPartitionsDefinition(name="nhl_season")