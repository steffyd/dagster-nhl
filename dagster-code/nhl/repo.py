from dagster import Definitions, multiprocess_executor
from assets import ASSETS
from schedules import SCHEDULES
from jobs import JOBS
from resources import RESOURCES
from sensors import SENSORS

defs = Definitions(
    assets=ASSETS,
    schedules=SCHEDULES,
    jobs=JOBS,
    resources=RESOURCES,
    sensors=SENSORS,
    executor=multiprocess_executor,
)
