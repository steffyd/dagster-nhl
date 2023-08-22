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


# write a utility that takes in a total value and a current value
# and returns True if the current value is the closest possible value
# to a multiple of 10% of the total value
def is_closest_to_ten_percent(total, current):
    return abs(current - (floor(current/total * 100) * total/100)) < abs(current - (floor(current/total * 100) + 1) * total/100)