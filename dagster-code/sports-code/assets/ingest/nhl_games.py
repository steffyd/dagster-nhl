from dagster import asset, AssetExecutionContext
from dagster_gcp import GCSResource
import requests
from ..partitions import nhl_daily_partition
from utils.nhl_api import get_schedule

@asset(partitions_def=nhl_daily_partition)
def nhl_game_data(context: AssetExecutionContext, gcs: GCSResource):
    
    pass