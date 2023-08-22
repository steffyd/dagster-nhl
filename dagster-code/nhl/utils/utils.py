from datetime import datetime, timedelta
from dagster import OpExecutionContext
from math import floor

# helper function that takes in an OpExecutionContext
# and returns the datetimes for a partition starttime and endtime
def get_partition_time_range(context : OpExecutionContext) -> (datetime, datetime):
    start_time, end_time = context.asset_partition_key_range
    query_start_time: datetime = datetime.strptime(start_time, '%Y-%m-%d')
    query_end_time: datetime = datetime.strptime(end_time, '%Y-%m-%d')
    
    # if it's the same time, we actually want to grab the last hour worth of data
    if query_start_time == query_end_time:
        query_end_time = query_end_time + timedelta(hours=1)
    
    context.log.info({"start_time": query_start_time.strftime('%Y-%m-%d'),
                        "end_time": query_end_time.strftime('%Y-%m-%d')})
    return query_start_time, query_end_time


# write a utility that takes the total number of items
# and the current index and returns true if the current index
# is closest to a percentage_value increment
def is_closest_to_percentage_increment(total_items: int, current_index: int, percentage_value: int) -> bool:
    current_val = floor(current_index/total_items * 100) % percentage_value == 0
    prev_val = floor((current_index-1)/total_items * 100) % percentage_value == 0
    # return only if the current index is closest to a percentage_value increment
    return current_val and not prev_val