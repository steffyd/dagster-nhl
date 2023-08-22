import pandas as pd
from dagster import asset, Output, FreshnessPolicy, AutoMaterializePolicy
from utils.nhl_api import get_team_stats
from assets.partitions import nhl_daily_partition
from utils.utils import is_closest_to_ten_percent

@asset(
        io_manager_key="warehouse_io_manager",
        partitions_def=nhl_daily_partition,
        compute_kind="api",
        metadata={"partition_expr": "game_date"},
        output_required=False,
        freshness_policy=FreshnessPolicy(maximum_lag_minutes=30),
        auto_materialize_policy=AutoMaterializePolicy.eager()
)
def game_data_raw(context, schedule_raw: pd.DataFrame):
    # iterate over the schedule_raw dataframe and 
    # call the get_team_stats function for each game
    # and add the results to the game_data dataframe
    game_data = pd.DataFrame()
    context.log.info(f"Retrieving game data for {len(schedule_raw)} games")
    for index, row in schedule_raw.iterrows():
        team_stats_for_game = get_team_stats(row["game_id"])
        team_stats_for_game["game_date"] = row["game_date"]
        team_stats_for_game["game_type"] = row["game_type"]
        team_stats_for_game["season"] = row["season"]
        game_data = pd.concat([game_data, team_stats_for_game], ignore_index=True)
        # only log out near 10% of the schedule
        if is_closest_to_ten_percent(len(schedule_raw), index):
            context.log.info(f"Retrieved game data for {index} games")

    if not game_data.empty:
        yield Output(game_data, metadata={"game_data_count":len(game_data)})
    else:
        context.log.info("No game data found for the given time range")