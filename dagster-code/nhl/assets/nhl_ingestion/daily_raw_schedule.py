from dagster import asset, Output
import pandas as pd
from datetime import timedelta
from utils.nhl_api import get_schedule_expanded
from utils.utils import get_partition_time_range
from assets.partitions import nhl_future_week_daily_partition


@asset(
        io_manager_key="warehouse_io_manager",
        partitions_def=nhl_future_week_daily_partition,
        compute_kind="api",
        metadata={"partition_expr": "TIMESTAMP_SECONDS(game_date)"},
        key_prefix="nhl_ingestion"
)
def schedule_raw(context):
    start_time, end_time = get_partition_time_range(context)
    # get the total number of days that we're running
    total_days = (end_time - start_time).days + 1
    # get each day between the start_time and end_time
    # and call the get_schedule_expanded function for each day
    schedules = []
    context.log.info(f"Retrieving schedule for {total_days} days")
    count = 0
    while start_time <= end_time:
        schedules.append(get_schedule_expanded(start_time.strftime('%Y-%m-%d'), context))
        if count % 10 == 0:
            context.log.info(f"Retrieved schedule for {count} days")
        start_time = start_time + timedelta(days=1)
        count += 1

    return Output(pd.DataFrame(schedules), metadata={"schedule_count":len(schedules)})