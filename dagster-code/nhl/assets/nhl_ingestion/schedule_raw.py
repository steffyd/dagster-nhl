from dagster import asset, Output
import pandas as pd
from datetime import timedelta
from utils.nhl_api import get_schedule_expanded
from utils.utils import get_partition_time_range
from assets.partitions import nhl_future_week_daily_partition
from math import floor


@asset(
        io_manager_key="warehouse_io_manager",
        partitions_def=nhl_future_week_daily_partition,
        compute_kind="api",
        metadata={"partition_expr": "game_date"}
)
def schedule_raw(context):
    start_time, end_time = get_partition_time_range(context)
    # get the total number of days that we're running
    total_days = (end_time - start_time).days + 1
    # get each day between the start_time and end_time
    # and call the get_schedule_expanded function for each day
    schedules = pd.DataFrame()
    context.log.info(f"Retrieving schedule for {total_days} days")
    count = 0
    while start_time <= end_time:
        # add the schedule for the day to the schedules dataframe
        schedules = pd.concat([schedules,get_schedule_expanded(start_time.strftime('%Y-%m-%d'), context)], ignore_index=True)
        # only log out near 10% of the schedule
        if floor(count/total_days * 100) % 10 == 0:
            context.log.info(f"Retrieved schedule for {count} days")
        start_time = start_time + timedelta(days=1)
        count += 1
        
    return Output(schedules, metadata={"schedule_count":len(schedules)})