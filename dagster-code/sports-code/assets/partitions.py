from dagster import WeeklyPartitionsDefinition, TimeWindowPartitionMapping


nhl_weekly_partition = WeeklyPartitionsDefinition(start_date="1990-09-02", timezone=f"US/Pacific")

class LastPartitionMapping(TimeWindowPartitionMapping):
    def get_upstream_partitions_for_partition_range(
        self,
        downstream_partition_key_range,
        downstream_partitions_def,
        upstream_partitions_def,
    ):
        # Only materialize the last partition
        return [downstream_partition_key_range.end]