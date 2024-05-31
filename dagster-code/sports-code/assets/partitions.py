from dagster import WeeklyPartitionsDefinition, TimeWindowPartitionMapping


nhl_weekly_partition = WeeklyPartitionsDefinition(start_date="1990-09-02", timezone=f"US/Pacific")
