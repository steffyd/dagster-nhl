from dagster import load_assets_from_package_module
from . import nhl_ingestion
from . import dbt_assets
from . import nfl_ingestion
from . import general_sports


nhl_ingestion_assets = load_assets_from_package_module(
    package_module=nhl_ingestion, group_name="nhl_ingestion"
)

nfl_ingestions_assets = load_assets_from_package_module(package_module=nfl_ingestion, group_name="nfl_ingestion")

general_sports_assets = load_assets_from_package_module(package_module=general_sports, group_name="general_sports")

dbt_assets = load_assets_from_package_module(dbt_assets, group_name="dbt_assets")

ASSETS = [*nhl_ingestion_assets, *dbt_assets, *nfl_ingestions_assets, *general_sports_assets]