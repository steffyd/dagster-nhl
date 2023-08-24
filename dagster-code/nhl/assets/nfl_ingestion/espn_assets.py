from dagster import asset, Output, FreshnessPolicy, AutoMaterializePolicy
import requests
import pandas as pd

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
    # add the player_id as an index
    espn_player_ids = espn_player_ids.set_index("player_id")
    return Output(espn_player_ids, metadata={"espn_player_ids_count":len(espn_player_ids)})