from .slack_sensors import slack_on_run_failure
from .update_nhl_season_partitions import update_nhl_season_partitions

SENSORS = [slack_on_run_failure, update_nhl_season_partitions]
