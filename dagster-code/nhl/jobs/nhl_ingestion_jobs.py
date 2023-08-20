from dagster import define_asset_job, AssetSelection
from assets.partitions import nhl_daily_partition, nhl_future_week_daily_partition

nhl_games_job = define_asset_job(
    "nhl_games_job",
    AssetSelection.keys("game_data_raw"),
    partitions_def=nhl_daily_partition
)

nhl_schedule_job = define_asset_job(
    name="nhl_schedule_job",
    selection=AssetSelection.keys("schedule_raw"),
    partitions_def=nhl_future_week_daily_partition
)
