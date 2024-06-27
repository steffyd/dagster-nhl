from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    FreshnessPolicy,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    AssetIn
)
import requests
from ..partitions import nhl_weekly_partition, nhl_season_partition, SeasonPartitionMapping
from datetime import datetime
import requests
import json

BASE_URL="https://api-web.nhle.com/v1/"

materialize_on_cron_policy = AutoMaterializePolicy.eager().with_rules(
    # try to materialize this asset if it hasn't been materialized since the last cron tick
    AutoMaterializeRule.materialize_on_cron(cron_schedule="0 0 * * 0",  # Run at midnight on Sundays
                                            timezone=f"US/Pacific"),
)


@asset(partitions_def=nhl_weekly_partition,
       io_manager_key="raw_nhl_data_partitioned_gcs_io_manager",
       output_required=False,
       group_name="nhl",
       compute_kind="API",
       freshness_policy=FreshnessPolicy(
        maximum_lag_minutes=10080 # 7 days freshness
       ),  
       auto_materialize_policy=materialize_on_cron_policy
)
def nhl_game_data(context: AssetExecutionContext):
    """
    This asset is the raw nhl game data for a given week. It is partitioned by weekly folders in GCS,
    and the data is stored as a raw json file in GCS with the following structure:
    dagster-storage-raw-nhl-data/nhl_game_data/{date}/{gameId}.json
    """
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
            try:
                game_data[(date,game_id)] = requests.get(f'{BASE_URL}gamecenter/{game_id}/boxscore').json()
            except:
                context.log.info(f'Error getting game data for {game_id}')
                context.log.info(requests.get(f'{BASE_URL}gamecenter/{game_id}/boxscore'))
            
        context.log.info(f'Yielding game data for {len(game_data)} games on {date}')
        yield Output(game_data)

@asset(
    partitions_def=nhl_season_partition,
    io_manager_key="nhl_season_partitioned_bigquery_io_manager",
    group_name="nhl",
    compute_kind="Python",
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    ins={"nhl_game_data": AssetIn("nhl_game_data", partition_mapping=SeasonPartitionMapping())},
    key_prefix=["nhl_lake"]
)
def nhl_game_data_by_season(context: AssetExecutionContext, nhl_game_data):
    """
    This asset is the raw nhl game data for a given season. It is stored in a bigquery table with the table name being:
    nhl_game_data_{season}
    """
    # get the season for each game in the nhl_game_data partitioned asset
    season_data = {}
    for game_id, game_data in nhl_game_data.items():
        season_data[game_id] = game_data['season']
    # get all distinct seasons
    seasons = set(season_data.values())
    # make sure that there is only one season in the data
    assert len(seasons) == 1, f"Data contains multiple seasons or no seasons: {seasons}"
    # nhl_game_data.items() has the json data for each game. we want to combine each of those items into a single
    # json array that we then load to bigquery
    nhl_game_data_json_array = []
    for game_id, game_data in nhl_game_data.items():
        nhl_game_data_json_array.append(game_data)
    # turn the array to a json object
    nhl_game_data_json = json.dumps(nhl_game_data_json_array)
    # now we want to load all the game_data into a bigquery table sharded by the season
    # so we yield a dictionary of season to game data
    yield Output(nhl_game_data_json)