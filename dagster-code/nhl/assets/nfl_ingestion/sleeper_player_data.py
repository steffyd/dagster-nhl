from dagster import asset, Output, FreshnessPolicy, AutoMaterializePolicy
import requests
import pandas as pd

@asset(
        io_manager_key="warehouse_io_manager",
        compute_kind="api",
        freshness_policy=FreshnessPolicy(maximum_lag_minutes=60*24, cron_schedule="0 0 * * *", cron_schedule_timezone="America/Denver"),
        auto_materialize_policy=AutoMaterializePolicy.eager()
)
def sleeper_player_data(context):
    # get the sleeper player data
    sleeper_player_data = requests.get("https://api.sleeper.app/v1/players/nfl").json()
    # convert the json data to a pandas dataframe
    sleeper_player_data = pd.DataFrame.from_dict(sleeper_player_data, orient='index')
    # add the player_id as a column
    sleeper_player_data["player_id"] = sleeper_player_data.index
    # add the player_id as an index
    sleeper_player_data = sleeper_player_data.set_index("player_id")

    return Output(sleeper_player_data, metadata={"sleeper_player_data_count":len(sleeper_player_data)})