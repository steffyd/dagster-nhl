from .slack_sensors import slack_on_run_failure
from .asset_sensors import clean_assets_sensor

SENSORS = [slack_on_run_failure, clean_assets_sensor]
