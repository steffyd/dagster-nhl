import json
from pathlib import Path

from dagster import OpExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from ..partitions import nhl_daily_partition
from utils.constants import DBT_PROJECT_DIR
from .NHLDagsterDbtTranslator import NHLDagsterDbtTranslator 



@dbt_assets(
    manifest=Path(DBT_PROJECT_DIR, "target", "manifest.json"),
    partitions_def=nhl_daily_partition,
    dagster_dbt_translator=NHLDagsterDbtTranslator(),
    select="game_data"
)
def dbt_assets(context: OpExecutionContext, dbt: DbtCliResource):
    time_window = context.asset_partitions_time_window_for_output(
        list(context.selected_output_names)[0]
    )

    dbt_vars = {
        "min_date": time_window.start.strftime('%Y-%m-%d'),
        "max_date": time_window.end.strftime('%Y-%m-%d')
    }
    dbt_build_args = ["build", "--vars", json.dumps(dbt_vars)]

    yield from dbt.cli(dbt_build_args, context=context).stream()

@dbt_assets(
    manifest=Path(DBT_PROJECT_DIR, "target", "manifest.json"),
    dagster_dbt_translator=NHLDagsterDbtTranslator(),
    select="nfl_features"
)
def nfl_assets(context: OpExecutionContext, dbt: DbtCliResource):
    dbt_build_args = ["build"]

    yield from dbt.cli(dbt_build_args, context=context).stream()