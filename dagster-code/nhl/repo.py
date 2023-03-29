from dagster import Definitions, multiprocess_executor
from assets import (nhl_ingestion_assets)
from schedules import SCHEDULES
from jobs import JOBS
from resources import RESOURCES
from sensors import SENSORS

all_assets = [
    *nhl_ingestion_assets
]


defs = Definitions(
    assets=all_assets,
    schedules=SCHEDULES,
    jobs=JOBS,
    resources=RESOURCES,
    sensors=SENSORS,
    executor=multiprocess_executor,
)