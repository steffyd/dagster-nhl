from dagster import asset, Output, FreshnessPolicy, AutoMaterializePolicy
import requests
import pandas as pd
from utils.utils import is_closest_to_percentage_increment
from math import floor

def translate_items_to_ids(items):
    # convert each item in items that looks like this:
    #  {"$ref":"http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4246273?lang=en&region=us"}
    # to this: 4246273
    return [int(item["$ref"].split("/")[-1].split("?")[0]) for item in items]

@asset(
        io_manager_key="warehouse_io_manager",
        compute_kind="api",
        description="All espn player ids for nfl players",
        freshness_policy=FreshnessPolicy(maximum_lag_minutes=60*24, cron_schedule="0 0 * * *", cron_schedule_timezone="America/Denver"),
        auto_materialize_policy=AutoMaterializePolicy.eager()
)
def espn_nfl_player_ids(context):
    # get all espn player ids
    espn_player_ids = requests.get("https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes?limit=1000").json()
    # get the initial page, total pages we need to iterate over
    total_pages = espn_player_ids["pageCount"]
    total_count = espn_player_ids["count"]
    context.log.info(f"Retrieving espn player ids for {total_pages} pages and {total_count} players")
    # get the initial page of espn player ids
    espn_player_ids = translate_items_to_ids(espn_player_ids["items"])
    # iterate over the remaining pages of espn player ids
    for page in range(2, total_pages + 1):
        espn_player_ids = espn_player_ids + translate_items_to_ids(requests.get(f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes?limit=1000&page={page}").json()["items"])

    # convert the list of espn player ids to a pandas dataframe
    espn_player_ids = pd.DataFrame(espn_player_ids, columns=["player_id"])
    # add an insert_date column
    espn_player_ids["insert_date"] = pd.to_datetime("today")
    # add the player_id as an index
    espn_player_ids = espn_player_ids.set_index("player_id")
    return Output(espn_player_ids, metadata={"espn_player_ids_count":len(espn_player_ids)})

@asset(
        io_manager_key="warehouse_io_manager",
        compute_kind="api",
        description="All espn player data for nfl players",
        freshness_policy=FreshnessPolicy(maximum_lag_minutes=60*24, cron_schedule="0 0 * * *", cron_schedule_timezone="America/Denver"),
        auto_materialize_policy=AutoMaterializePolicy.eager()
)
def espn_nfl_player(context, espn_nfl_player_ids):
    # get nfl athlete data for each espn player id
    espn_nfl_player_data = pd.DataFrame()
    total_items = len(espn_nfl_player_ids)
    context.log.info(f"Retrieving espn player data for {total_items} players")
    for index, row in espn_nfl_player_ids.iterrows():
        nfl_player_json = requests.get(f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/{row['player_id']}").json()
        nfl_player = pd.DataFrame()
        nfl_player["player_id"] = [index]
        nfl_player["player_name"] = [nfl_player_json["displayName"]]
        nfl_player["weight"] = [nfl_player_json["weight"]]
        nfl_player["height"] = [nfl_player_json["height"]]
        nfl_player["birth_date"] = [nfl_player_json["dateOfBirth"]]
        nfl_player["debut_year"] = [nfl_player_json["debutYear"]]
        nfl_player["position"] = [nfl_player_json["position"]["abbreviation"]]
        nfl_player["experience"] = [nfl_player_json["experience"]]
        nfl_player["status"] = [nfl_player_json["status"]["name"]]
        espn_nfl_player_data = pd.concat([pd.DataFrame(espn_nfl_player_data), nfl_player], ignore_index=True)
        # only log out near a set % chunk of the total items
        if is_closest_to_percentage_increment(total_items, index, 10):
            context.log.info(f"Retrieved espn player data for {floor(index/total_items * 100)}% of total players")
    
    # add an insert_date column
    espn_nfl_player_data["insert_date"] = pd.to_datetime("today")
    # add the player_id as an index
    espn_nfl_player_data = espn_nfl_player_data.set_index("player_id")
    return Output(espn_nfl_player_data, metadata={"espn_nfl_player_data_count":len(espn_nfl_player_data)})