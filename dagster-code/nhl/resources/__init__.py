from .postgres_resource import postgres_resource_by_db
from .postgres_partitioned_io_manager import postgres_partitioned_io_manager
from .coc_slack_resource import coc_slack_resource
from dagster_dbt import dbt_cli_resource
from utils.constants import DBT_PROFILES_DIR, DBT_PROJECT_DIR

RESOURCES = {
    "postgres_resource_by_db": postgres_resource_by_db,
    "postgres_partitioned_io_manager": postgres_partitioned_io_manager,
    "coc_slack_resource": coc_slack_resource,
    "dbt": dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_DIR,
            "profiles_dir": DBT_PROFILES_DIR,
        },
    ),
}
