from dagster import asset, AssetExecutionContext, Output
import requests
from ..partitions import nhl_daily_partition
from datetime import datetime
import requests

BASE_URL="https://api-web.nhle.com/v1/"

@asset(partitions_def=nhl_daily_partition, io_manager_key="partitioned_gcs_io_manager", output_required=False)
def nhl_game_data(context: AssetExecutionContext):
    # get the start and end partition as well as the total partition counts
    dates = [datetime.strptime(date_str, '%Y-%m-%d') for date_str in context.partition_keys]
    context.log.info(f'Getting game data for {len(dates)} days from {min(dates)} to {max(dates)}')
    
    for date in context.partition_keys:
        context.log.info(f'Getting game data for {date}')
        url = f"{BASE_URL}schedule/{date}"
        response = requests.get(url)
        # check if the response is successful
        if response.status_code != 200:
            context.log.info(f'No data for partition {date}')
            continue
        data = response.json()
        # get gamePks from the schedule response
        game_ids = [game['id'] for week in data['gameWeek'] for game in week['games']]

        context.log.info(f'Getting game data for {len(game_ids)} games')
        # now that we have the games for the day, get the game data
        # and yield a dictionary of gameId to game data
        game_data = {}
        for game_id in game_ids:
            game_data[(date,game_id)] = requests.get(f'{BASE_URL}gamecenter/{game_id}/boxscore').json()
            
        context.log.info(f'Yielding game data for {len(game_data)} games on {date}')
        yield Output(game_data)