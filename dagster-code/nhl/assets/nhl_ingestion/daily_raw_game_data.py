import datetime as dt
import pandas as pd
from dagster import (
    graph,
    op,
    Out,
    DynamicOut,
    DynamicOutput,
    AssetsDefinition,
    Output,
)
from utils.nhl_api import get_schedule, get_team_stats
from assets.partitions import nhl_daily_partition
from resources.coc_slack_resource import coc_slack_resource


@op(
    config_schema={
        "date": str,
    },
    out={"schedule": Out(), "date": Out()},
)
def daily_schedule(context):
    date = context.op_config["date"]
    return (get_schedule(date, context), date)


@op(out=DynamicOut())
def split_schedule_by_day(context, daily_schedule):
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
        io_manager_key="postgres_partitioned_io_manager",
        metadata={
            "database": "nhl_stats",
            "schema_name": "raw",
            "table_name": "game_data",
        },
    ),
    required_resource_keys={"slack"},
)
def daily_game_stats(context, date, game_stats: list[pd.DataFrame]):
    if not game_stats:
        return Output(pd.DataFrame(), metadata={"num_rows": 0})
    days_game_stats = pd.concat(game_stats)
    return Output(days_game_stats, metadata={"num_rows": days_game_stats.shape[0]})


@graph
def raw_game_data():
    schedule, date = daily_schedule()
    result = split_schedule_by_day(schedule).map(fetch_team_stats).collect()
    return daily_game_stats(date, result)


graph_asset = AssetsDefinition.from_graph(
    raw_game_data,
    partitions_def=nhl_daily_partition,
    resource_defs={"slack": coc_slack_resource},
    key_prefix="nhl_ingestion",
)
