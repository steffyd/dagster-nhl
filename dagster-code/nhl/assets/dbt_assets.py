import json
from pathlib import Path

from dagster import OpExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from .partitions import nhl_daily_partition


@dbt_assets(
    manifest=Path("target", "manifest.json"),
    partitions_def=nhl_daily_partition
)
def game_data(context: OpExecutionContext, dbt: DbtCliResource):
    time_window = context.asset_partitions_time_window_for_output(
        list(context.selected_output_names)[0]
    )

    dbt_vars = {
        "min_date": time_window.start.isoformat(),
        "max_date": time_window.end.isoformat()
    }
    dbt_build_args = ["build", "--vars", json.dumps(dbt_vars)]

    yield from dbt.cli(dbt_build_args, context=context).stream()