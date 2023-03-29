from dagster import load_assets_from_package_module
from dagster._utils import file_relative_path
from dagster_dbt import load_assets_from_dbt_project
from . import nhl_ingestion

DBT_PROJECT_DIR = file_relative_path(__file__, "../nhl_dbt")
DBT_PROFILES_DIR = DBT_PROJECT_DIR + "/config"

nhl_ingestion_assets = load_assets_from_package_module(
    package_module=nhl_ingestion, group_name="nhl_ingestion"
)

dbt_assets = load_assets_from_dbt_project(DBT_PROJECT_DIR)
