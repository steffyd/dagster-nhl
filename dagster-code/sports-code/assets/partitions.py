from dagster import WeeklyPartitionsDefinition, DynamicPartitionsDefinition, PartitionMapping, DagsterInstance
from dagster._core.definitions.partition_mapping import UpstreamPartitionsResult, PartitionsSubset
import datetime
import requests


class SeasonPartitionMapping(PartitionMapping):
    def description(self):
        return "Map NHL weekly partitions to NHL season partitions"

    def get_downstream_partitions_for_partitions(
        self,
        upstream_partitions_subset,
        upstream_partitions_def,
        downstream_partitions_def,
        current_time = None,
        dynamic_partitions_store = None,
    ):
        url = "https://api.nhle.com/stats/rest/en/season"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception("Failed to get season data")
        data = response.json()
        downstream_keys = set()
        for week in upstream_partitions_subset.get_partition_keys():
            week_date = datetime.datetime.strptime(week, "%Y-%m-%d")
            # get all ids and startDate and endDate for each value in the data array
            for season_data in data["data"]:
                # add to the upstream_keys the weeks that are in between the start and end date
                # for the given season
                start_date = datetime.datetime.strptime(season_data["startDate"], "%Y-%m-%d")
                end_date = datetime.datetime.strptime(season_data["endDate"], "%Y-%m-%d")
                if start_date <= week_date <= end_date:
                    downstream_keys.add(str(season_data["id"]))
                    continue
                            
        return downstream_partitions_def.empty_subset().with_partition_keys(downstream_keys)

    def get_upstream_mapped_partitions_result_for_partitions(
        self,
        downstream_partitions_subset,
        downstream_partitions_def,
        upstream_partitions_def,
        current_time = None,
        dynamic_partitions_store = None,
    ) :
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
                if str(season_data["id"]) == season:
                    start_date = datetime.datetime.strptime(season_data["startDate"].split("T")[0], "%Y-%m-%d")
                    end_date = datetime.datetime.strptime(season_data["endDate"].split("T")[0], "%Y-%m-%d")
                    for week in upstream_partitions_def.get_partition_keys():
                        week_date = datetime.datetime.strptime(week, "%Y-%m-%d")
                        if start_date <= week_date <= end_date:
                            upstream_keys.add(week)
        
        return UpstreamPartitionsResult(
            partitions_subset=upstream_partitions_def.empty_subset().with_partition_keys(upstream_keys),
            required_but_nonexistent_partition_keys=set(),
        )


nhl_weekly_partition = WeeklyPartitionsDefinition(start_date="1990-09-02", timezone=f"US/Pacific")
nhl_season_partition = DynamicPartitionsDefinition(name="nhl_season")