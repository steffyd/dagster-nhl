from dagster import load_assets_from_package_module
from . import general_sports

general_sports_assets = load_assets_from_package_module(package_module=general_sports, group_name="general_sports")

ASSETS = [*general_sports_assets]