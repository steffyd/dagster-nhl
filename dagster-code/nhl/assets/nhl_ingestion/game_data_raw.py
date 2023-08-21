import datetime as dt
import pandas as pd
from dagster import (
    graph_asset,
    op,
    Out,
    DynamicOut,
    DynamicOutput,
    Output,
)
from utils.nhl_api import get_schedule, get_team_stats
from utils.utils import get_partition_time_range
from assets.partitions import nhl_daily_partition


@op
def daily_schedule(context):
    start_date, end_date = get_partition_time_range(context)
    schedule = pd.DataFrame()
    while start_date <= end_date:
        schedule = pd.concat([schedule, get_schedule(start_date.strftime('%Y-%m-%d'), context)], ignore_index=True)
        start_date = start_date + dt.timedelta(days=1)
    return schedule


@op(out=DynamicOut())
def split_schedule_by_game(context, daily_schedule):
    for index, row in daily_schedule.iterrows():
        yield DynamicOutput(row, mapping_key=str(index))


@op
def fetch_team_stats(context, schedule_row) -> pd.DataFrame:
    game_id = schedule_row["game_id"]
    team_stats_for_game = get_team_stats(game_id)
    team_stats_for_game["game_date"] = schedule_row["game_date"]
    team_stats_for_game["game_type"] = schedule_row["game_type"]
    team_stats_for_game["season"] = schedule_row["season"]
    return team_stats_for_game


@op(
    out=Out(
        io_manager_key="warehouse_io_manager",
        metadata={"partition_expr": "game_date"}
    )
)
def daily_game_stats(context, game_stats: list[pd.DataFrame]):
    if not game_stats:
        return Output(pd.DataFrame(), metadata={"num_rows": 0})
    days_game_stats = pd.concat(game_stats)
    return Output(days_game_stats, metadata={"num_rows": days_game_stats.shape[0]})


@graph_asset(partitions_def=nhl_daily_partition)
def game_data_raw():
    result = split_schedule_by_game(daily_schedule()).map(fetch_team_stats).collect()
    return daily_game_stats(result)