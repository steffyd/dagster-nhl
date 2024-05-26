from dagster import asset, DailyPartitionsDefinition, AssetIn, LastPartitionMapping, AssetExecutionContext
from resources.espn_api_resource import EspnApiResource
import pandas as pd

daily_schedule_partitions = DailyPartitionsDefinition(start_date="2021-01-01", end_offset=1)

def generate_list_from_college_football_json(json):
    # generate a list of the event objects we want
    event_data = []
    for event in json["events"]:
        # get shortName, competitions[0]:venue.fullName, venue.indoor, competitors[0].records[0].summary, competitors[1].records[0].summary, startDate, broadcasts[0].names[0], broadcasts[0].names[1], tickets[0].summary
        temp_event_data = {}
        temp_event_data["short_name"] = [event["shortName"]]
        temp_event_data["venue"] = [event["competitions"][0]["venue"]["fullName"]]
        temp_event_data["indoor"] = [event["competitions"][0]["venue"]["indoor"]]
        temp_event_data["home_team"] = [event["competitions"][0]["competitors"][0]["team"]["shortDisplayName"]]
        temp_event_data["away_team"] = [event["competitions"][0]["competitors"][1]["team"]["shortDisplayName"]]
        temp_event_data["home_team_record"] = [event["competitions"][0]["competitors"][0]["records"][0]["summary"]]
        temp_event_data["away_team_record"] = [event["competitions"][0]["competitors"][1]["records"][0]["summary"]]
        temp_event_data["start_date"] = [event["competitions"][0]["startDate"]]
        temp_event_data["broadcast_network"] = [event["competitions"][0]["broadcasts"][0]["names"][0]]
        temp_event_data["broadcast_type"] = [event["competitions"][0]["broadcasts"][0]["names"][1]]
        temp_event_data["tickets"] = [event["competitions"][0]["tickets"][0]["summary"]]
    return event_data

@asset(partitions_def=daily_schedule_partitions)
def college_football_schedule(context: AssetExecutionContext, espn_api: EspnApiResource):
    starting_partition = context.partition_key_range.start
    ending_partition = context.partition_key_range.end
    context.log.info(f"Starting partition: {starting_partition}, Ending partition: {ending_partition}")
    final_data_list = []
    if starting_partition == ending_partition:
        date = starting_partition
        raw_data = espn_api.get_scoreboard_for_date("football", "college-football", date)
        event_data = generate_list_from_college_football_json(raw_data)
        final_data_list.append(event_data)
    else:
        for date in context.partition_keys:
            context.log.info(f"Getting data for date: {date}")
            raw_data = espn_api.get_scoreboard_for_date("football", "college-football", date)
            event_data = generate_list_from_college_football_json(raw_data)
            final_data_list.append(event_data)
    final_data_df = pd.DataFrame(final_data_list)
    return final_data_df

@asset(partitions_def=daily_schedule_partitions)
def nhl_schedule(context, espn_api: EspnApiResource):
    pass

@asset(partitions_def=daily_schedule_partitions)
def nfl_schedule(context, espn_api: EspnApiResource):
    pass

@asset(partitions_def=daily_schedule_partitions)
def premiere_league_schedule(context, espn_api: EspnApiResource):
    pass

# @asset(ins={"nfl_sched": AssetIn(key="nfl_schedule",partition_mapping=LastPartitionMapping()),
#             "nhl_sched": AssetIn(key="nhl_schedule",partition_mapping=LastPartitionMapping()),
#             "premier_league_sched": AssetIn(key="premiere_league_schedule",partition_mapping=LastPartitionMapping()),
#             "college_football_sched": AssetIn(key="college_football_schedule",partition_mapping=LastPartitionMapping())}
#       )
# def current_sports_schedule(context):
#     pass