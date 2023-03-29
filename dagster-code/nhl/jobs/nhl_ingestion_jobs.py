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


@daily_partitioned_config(start_date="1990-08-01", end_offset=7)
def nhl_schedule_daily_partitioned_config(start: datetime, _end: datetime):
    return {
        "ops": {
            "raw_schedule_data": {
                "ops": {
                    "daily_schedule_expanded": {
                        "config": {"date": start.strftime("%Y-%m-%d")}
                    }
                }
            }
        }
    }


daily_nhl_games_job = define_asset_job(
    "daily_nhl_games_job",
    AssetSelection.keys("raw_game_data"),
    partitions_def=nhl_daily_partition,
    config=nhl_daily_partitioned_config,
)

daily_nhl_schedule_job = define_asset_job(
    "daily_nhl_schedule_job",
    AssetSelection.keys("raw_schedule_data"),
    partitions_def=nhl_future_week_daily_partition,
    config=nhl_schedule_daily_partitioned_config,
)
