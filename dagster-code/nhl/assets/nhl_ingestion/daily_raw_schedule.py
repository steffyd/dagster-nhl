import datetime as dt
import pandas as pd
from dagster import (
    graph,
    op,
    Out,
    AssetsDefinition,
    Output
)
from utils.nhl_api import get_schedule_expanded
from assets.partitions import nhl_future_week_daily_partition


@op(
    out=Out(
        io_manager_key="postgres_partitioned_io_manager",
        metadata={
            "database": "nhl_stats",
            "schema_name": "raw",
            "table_name": "schedule_data",
        },
    ),
    config_schema={
        "date": str,
    },
)
def daily_schedule_expanded(context):
    date = context.op_config["date"]
    schedule = get_schedule_expanded(date, context)
    return Output(schedule, metadata={"num_games": schedule.shape[0]})

@graph
def raw_schedule_data():
    return daily_schedule_expanded()

raw_schedule_asset = AssetsDefinition.from_graph(
    raw_schedule_data,
    partitions_def=nhl_future_week_daily_partition
)