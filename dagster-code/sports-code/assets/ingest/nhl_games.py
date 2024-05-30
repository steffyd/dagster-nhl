from dagster import asset, AssetExecutionContext
import requests
from ..partitions import nhl_daily_partition
from utils.nhl_api import get_schedule, BASE_URL
from datetime import datetime

@asset(partitions_def=nhl_daily_partition, io_manager_key="partitioned_gcs_io_manager")
def nhl_game_data(context: AssetExecutionContext):
    # get the start and end partition as well as the total partition counts
    dates = [datetime.strptime(date_str, '%Y-%m-%d') for date_str in context.partition_keys]
    context.log.info(f'Getting game data for {len(dates)} days from {min(dates)} to {max(dates)}')
    
    for date in context.partition_keys:
        context.log.info(f'Getting game data for {date}')
        schedule = get_schedule(date)
        # now that we have the games for the day, get the game data
        # and yield a dictionary of gameId to game data
        game_data = {}
        for game in schedule:
            context.log.info(f'Getting game data for {game["gamePk"]}')
            game_id = game['gamePk']
            game_data[game_id] = requests.get(f'{BASE_URL}game/{game_id}/feed/live').json()
        yield game_data
