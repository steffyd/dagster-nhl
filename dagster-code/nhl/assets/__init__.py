from dagster import load_assets_from_package_module
from . import nhl_ingestion

nhl_ingestion_assets = load_assets_from_package_module(package_module=nhl_ingestion, group_name="nhl_ingestion")