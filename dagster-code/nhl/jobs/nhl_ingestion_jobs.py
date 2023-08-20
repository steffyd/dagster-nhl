from dagster import define_asset_job, AssetSelection, daily_partitioned_config
from assets.partitions import nhl_daily_partition, nhl_future_week_daily_partition
import datetime


@daily_partitioned_config(start_date="1990-08-01")
def nhl_daily_partitioned_config(start: datetime, _end: datetime):
    return {
        "ops": {
            "raw_game_data": {
                "ops": {
                    "daily_schedule": {"config": {"date": start.strftime("%Y-%m-%d")}}
                }
            }
        }
    }

daily_nhl_games_job = define_asset_job(
    "daily_nhl_games_job",
    AssetSelection.keys("nhl_ingestion/raw_game_data"),
    partitions_def=nhl_daily_partition,
    config=nhl_daily_partitioned_config,
)

nhl_schedule_job = define_asset_job(
    name="nhl_schedule_job",
    selection=AssetSelection.keys("schedule_raw"),
    partitions_def=nhl_future_week_daily_partition
)
