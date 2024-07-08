from dagster import (
    asset,
    Output,
    FreshnessPolicy,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    BackfillPolicy
)
import requests
from datetime import datetime
from ..partitions import nhl_weekly_partition
from unittest.mock import patch, MagicMock

BASE_URL = "https://api-web.nhle.com/v1/"

materialize_on_cron_policy = AutoMaterializePolicy.eager().with_rules(
    AutoMaterializeRule.materialize_on_cron(cron_schedule="0 0 * * 0",  # Run at midnight on Sundays
                                            timezone="US/Pacific"),
)

@asset(
    partitions_def=nhl_weekly_partition,
    io_manager_key="raw_nhl_data_partitioned_gcs_io_manager",
    group_name="nhl",
    compute_kind="API",
    freshness_policy=FreshnessPolicy(
        maximum_lag_minutes=10080  # 7 days freshness
    ),
    auto_materialize_policy=materialize_on_cron_policy
)
def future_games(context):
    dates = [datetime.strptime(date_str, '%Y-%m-%d') for date_str in context.partition_keys]
    for date in context.partition_keys:
        url = f"{BASE_URL}schedule/{date}"
        response = requests.get(url)
        if response.status_code != 200:
            continue
        data = response.json()
        game_data = extract_future_game_data(data)
        yield Output(game_data)

def extract_future_game_data(data):
    game_data_list = []
    for game_week in data.get('gameWeek', []):
        date = game_week.get('date')
        for game in game_week.get('games', []):
            game_info = {
                'date': date,
                'gameId': game.get('id'),
                'awayTeam': game.get('awayTeam', {}).get('abbrev'),
                'homeTeam': game.get('homeTeam', {}).get('abbrev')
            }
            game_data_list.append(game_info)
    return game_data_list

