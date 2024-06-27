from dagster import schedule, ScheduleEvaluationContext
from assets.partitions import nhl_season_partition
import requests

@schedule(
    cron_schedule="0 6 * * *"
)
def update_nhl_season_partitions(context: ScheduleEvaluationContext):
    """
    Schedule to update NHL season partitions with new seasons
    """
    url = "https://api.nhle.com/stats/rest/en/season"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Failed to get season data")
    data = response.json()
    season_ids = [season["id"] for season in data["data"]]
    context.log.info(f"Adding {len(season_ids)} season partitions")
    for season_id in season_ids:
        if season_id not in context.instance.get_dynamic_partitions(nhl_season_partition.name):
            context.log.info(f"Adding season partition {season_id}")
            context.instance.add_dynamic_partition(nhl_season_partition.name, season_id)