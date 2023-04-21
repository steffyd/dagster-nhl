from dagster import load_assets_from_modules, load_assets_from_package_module
from . import nhl_ingestion
from . import dbt_assets


nhl_ingestion_assets = load_assets_from_package_module(
    package_module=nhl_ingestion, group_name="nhl_ingestion"
)

dbt_assets = load_assets_from_modules([dbt_assets])

ASSETS = [*nhl_ingestion_assets, *dbt_assets]