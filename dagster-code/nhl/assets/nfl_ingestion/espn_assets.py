from dagster import asset, Output, FreshnessPolicy, AutoMaterializePolicy
import requests
import pandas as pd
from utils.utils import is_closest_to_percentage_increment, get_json_field, get_nested_json_field
from math import floor
import re

def translate_items_to_ids(items):
    # convert each item in items that looks like this:
    #  {"$ref":"http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4246273?lang=en&region=us"}
    # to this: 4246273
    # using regex
    return [re.search(r"athletes\/(\d+)", item["$ref"]).group(1) for item in items]

@asset(
        io_manager_key="warehouse_io_manager",
        compute_kind="api",
        description="All espn player ids for nfl players",
        freshness_policy=FreshnessPolicy(maximum_lag_minutes=60*24, cron_schedule="0 0 * * *", cron_schedule_timezone="America/Denver"),
        auto_materialize_policy=AutoMaterializePolicy.eager(),
        required_resource_keys={"bigquery"}
)
def espn_nfl_player_ids(context):
    # clear the espn_nfl_player_ids table
     with context.resources.bigquery.get_client() as client:
         client.query("DELETE FROM `corellian-engineering-co.NHLData.espn_nfl_player_ids`")

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
        auto_materialize_policy=AutoMaterializePolicy.eager(),
        required_resource_keys={"bigquery"}
)
def espn_nfl_player(context, espn_nfl_player_ids):
    # clear the espn_nfl_player_data table
    with context.resources.bigquery.get_client() as client:
         client.query("DELETE FROM `corellian-engineering-co.NHLData.espn_nfl_player_data`")
    # get nfl athlete data for each espn player id
    espn_nfl_player_data = pd.DataFrame()
    total_items = len(espn_nfl_player_ids)
    context.log.info(f"Retrieving espn player data for {total_items} players")
    for index, row in espn_nfl_player_ids.iterrows():
        player_id = row['player_id']
        nfl_player_json = requests.get(f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/{player_id}").json()
        nfl_player = pd.DataFrame()
        nfl_player["player_id"] = [player_id]
        nfl_player["player_name"] = [nfl_player_json["displayName"]]
        # check if the player has weight, height, dateOfBirth, debutYear, position, experience, and status
        # if not, set the value to None
        nfl_player["weight"] = [get_json_field(nfl_player_json, "weight")]
        nfl_player["height"] = [get_json_field(nfl_player_json,"height")]
        nfl_player["birth_date"] = [get_json_field(nfl_player_json,"dateOfBirth")]
        nfl_player["debut_year"] = [get_json_field(nfl_player_json,"debutYear")]
        nfl_player["position"] = [get_nested_json_field(nfl_player_json,"position","abbreviation")]
        nfl_player["experience"] = [nfl_player_json["experience"]]
        nfl_player["status"] = [get_nested_json_field(nfl_player_json,"status","name")]
        espn_nfl_player_data = pd.concat([pd.DataFrame(espn_nfl_player_data), nfl_player], ignore_index=True)
        # only log out near a set % chunk of the total items
        if is_closest_to_percentage_increment(total_items, index, 10):
            context.log.info(f"Retrieved espn player data for {floor(index/total_items * 100)}% of total players")
    
    # add an insert_date column
    espn_nfl_player_data["insert_date"] = pd.to_datetime("today")
    # add the player_id as an index
    espn_nfl_player_data = espn_nfl_player_data.set_index("player_id")
    return Output(espn_nfl_player_data, metadata={"espn_nfl_player_data_count":len(espn_nfl_player_data)})

@asset(
        io_manager_key="warehouse_io_manager",
        compute_kind="api",
        description="All espn player stats for nfl players",
        freshness_policy=FreshnessPolicy(maximum_lag_minutes=60*24, cron_schedule="0 0 * * *", cron_schedule_timezone="America/Denver"),
        auto_materialize_policy=AutoMaterializePolicy.eager(),
        required_resource_keys={"bigquery"}
)
def espn_nfl_player_stats_by_season(context, espn_nfl_player_ids):
    # clear the espn_nfl_player_stats_by_season table
    with context.resources.bigquery.get_client() as client:
         client.query("DELETE FROM `corellian-engineering-co.NHLData.espn_nfl_player_stats_by_season`")
    # get the nfl players stat log for each espn player id
    espn_nfl_player_stats_by_season = pd.DataFrame()
    total_items = len(espn_nfl_player_ids)
    context.log.info(f"Retrieving espn player stats by season for {total_items} players")
    for index, row in espn_nfl_player_ids.iterrows():
        player_id = row['player_id']
        try:
            seasons = get_json_field(requests.get(f"http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/{player_id}/statisticslog?lang=en&region=us").json(), "entries")
            if seasons is None:
                continue
            for season in seasons:
                url = season["statistics"][0]["statistics"]["$ref"]
                # extract the season from the url using regex, which looks like this: http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2020/types/2/athletes/2330/statistics?lang=en&region=us
                season = re.search(r"seasons\/(\d+)", url).group(1)            
                season_player_stats = requests.get(url).json()
                season_stat = pd.DataFrame()
                season_stat["player_id"] = [player_id]
                season_stat["season"] = [season]
                if "splits" not in season_player_stats:
                    continue
                for category in season_player_stats["splits"]["categories"]:
                    for stats in category["stats"]:
                        stat_name = stats["name"]
                        stat_value = stats["value"]
                        season_stat[stat_name] = [stat_value]
                espn_nfl_player_stats_by_season = pd.concat([pd.DataFrame(espn_nfl_player_stats_by_season), season_stat], ignore_index=True)
            # only log out near a set % chunk of the total items
            if is_closest_to_percentage_increment(total_items, index, 10):
                context.log.info(f"Retrieved espn player stats by season for {floor(index/total_items * 100)}% of total players")
        except:
            context.log.info(f"Could not retrieve espn player stats by season for player_id: {player_id}")
            raise
    
    # add an insert_date column
    espn_nfl_player_stats_by_season["insert_date"] = pd.to_datetime("today")
    # add the player_id as an index
    espn_nfl_player_stats_by_season = espn_nfl_player_stats_by_season.set_index(["player_id", "season"])
    return Output(espn_nfl_player_stats_by_season, metadata={"espn_nfl_player_stats_by_season_count":len(espn_nfl_player_stats_by_season)})
