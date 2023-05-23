from dagster_dbt import load_assets_from_dbt_project
from utils.constants import DBT_PROJECT_DIR
from .partitions import nhl_daily_partition

def partition_key_to_vars(partition_key):
    """ Map dagster partitions to the dbt var used in our model WHERE clauses """
    return {"datetime_to_process": partition_key}

dbt_assets = load_assets_from_dbt_project(DBT_PROJECT_DIR,
                                          partitions_def= nhl_daily_partition,
                                          partition_key_to_vars_fn=partition_key_to_vars,
                                          select="game_data team_stats_windowed_82_games team_season_totals_by_date")
