from dagster import WeeklyPartitionsDefinition, DynamicPartitionsDefinition
from dagster import PartitionMapping, PartitionsDefinition, PartitionsSubset, UpstreamPartitionsResult
from dagster.core.storage.dynamic import DynamicPartitionsStore
from typing import Optional
import datetime
import requests


class SeasonPartitionMapping(PartitionMapping):
    def get_upstream_partitions_for_partition_range(
        self,
        downstream_partitions_subset: PartitionsSubset,
        downstream_partitions_def: PartitionsDefinition,
        upstream_partitions_def: PartitionsDefinition,
        current_time: Optional[datetime.datetime] = None,
        dynamic_partitions_store: Optional[DynamicPartitionsStore] = None,
    ) -> UpstreamPartitionsResult:
        # Map downstream partitions to upstream partitions based on the season value
        upstream_keys = set()
        for season in downstream_partitions_subset.get_partition_keys():
            url = "https://api.nhle.com/stats/rest/en/season"
            response = requests.get(url)
            if response.status_code != 200:
                continue
            data = response.json()
            # get all ids and startDate and endDate for each value in the data array
            for season_data in data["data"]:
                # add to the upstream_keys the weeks that are in between the start and end date
                # for the given season
                if season_data["id"] == season:
                    start_date = datetime.datetime.strptime(season_data["startDate"], "%Y-%m-%d")
                    end_date = datetime.datetime.strptime(season_data["endDate"], "%Y-%m-%d")
                    for week in upstream_partitions_def.get_partition_keys():
                        week_date = datetime.datetime.strptime(week, "%Y-%m-%d")
                        if start_date <= week_date <= end_date:
                            upstream_keys.add(week)
        
        return UpstreamPartitionsResult(
            partitions_subset=upstream_partitions_def.empty_subset().with_partition_keys(upstream_keys)
        )


nhl_weekly_partition = WeeklyPartitionsDefinition(start_date="1990-09-02", timezone=f"US/Pacific")
nhl_season_partition = DynamicPartitionsDefinition(name="nhl_season")