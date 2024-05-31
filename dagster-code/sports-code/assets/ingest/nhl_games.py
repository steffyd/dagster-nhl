from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    FreshnessPolicy,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    LastPartitionMapping,
    AssetIn
)
import requests
from ..partitions import nhl_weekly_partition
from datetime import datetime
import requests
from bigquery_schema_generator.generate_schema import SchemaGenerator

BASE_URL="https://api-web.nhle.com/v1/"

materialize_on_cron_policy = AutoMaterializePolicy.eager().with_rules(
    # try to materialize this asset if it hasn't been materialized since the last cron tick
    AutoMaterializeRule.materialize_on_cron(cron_schedule="0 0 * * 0",  # Run at midnight on Sundays
                                            timezone=f"US/Pacific"),
)


@asset(partitions_def=nhl_weekly_partition,
       io_manager_key="partitioned_gcs_io_manager",
       output_required=False,
       group_name="nhl",
       compute_kind="API",
       freshness_policy=FreshnessPolicy(
        maximum_lag_minutes=10080 # 7 days freshness
       ),  
       auto_materialize_policy=materialize_on_cron_policy
)
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

@asset(
        ins={"nhl_game_data": AssetIn(partition_mapping=LastPartitionMapping())},
        auto_materialize_policy=AutoMaterializePolicy.eager(),
        group_name="nhl",
        compute_kind="python"
)
def latest_nhl_schema(context: AssetExecutionContext, nhl_game_data: dict):
    # get the first game data to deduce the schema
    game_id, game_data = nhl_game_data.popitem()
    generator = SchemaGenerator()
    schema_map, error_logs = generator.deduce_schema(nhl_game_data[0])
    schema = generator.flatten_schema(schema_map)
    context.log.info(schema)
    return schema