from dagster import sensor, SensorEvaluationContext, SensorResult
from assets.partitions import nhl_season_partition
import requests

@sensor(
    minimum_interval_seconds=86400 # 1 day interval
)
def update_nhl_season_partitions(context: SensorEvaluationContext):
    """
    Sensor to update NHL season partitions with new seasons
    """
    url = "https://api.nhle.com/stats/rest/en/season"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Failed to get season data")
    data = response.json()
    season_ids = [season["id"] for season in data["data"]]

    new_seasons = []
    for season_id in season_ids:
        if season_id not in context.instance.get_dynamic_partitions(nhl_season_partition.name):
            context.log.info(f"Adding season partition {season_id}")
            new_seasons.append(season_id)


    context.log.info(f"Adding {len(new_seasons)} season partitions")
    return SensorResult(
        dynamic_partitions_requests=[
            nhl_season_partition.build_add_request(new_seasons)
        ],
    )