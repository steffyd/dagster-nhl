from dagster import load_assets_from_package_module
from dagster_dbt import load_assets_from_dbt_project
from . import nhl_ingestion
from utils.constants import DBT_PROJECT_DIR


nhl_ingestion_assets = load_assets_from_package_module(
    package_module=nhl_ingestion, group_name="nhl_ingestion"
)

dbt_assets = load_assets_from_dbt_project(DBT_PROJECT_DIR)
