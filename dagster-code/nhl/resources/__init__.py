from .postgres_resource import postgres_resource_by_db
from .postgres_partitioned_io_manager import postgres_partitioned_io_manager
from .coc_slack_resource import coc_slack_resource
from dagster import EnvVar
from dagster_dbt import DbtCliResource
from utils.constants import DBT_PROJECT_DIR
from dagster_gcp_pandas import BigQueryPandasIOManager
import os

RESOURCES = {
    "postgres_resource_by_db": postgres_resource_by_db,
    "postgres_partitioned_io_manager": postgres_partitioned_io_manager,
    "coc_slack_resource": coc_slack_resource,
    "dbt": DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR), profile="prod"),
    "warehouse_io_manager": BigQueryPandasIOManager(
        project="corellian-engineering-co",
        dataset="NHLData",
        gcp_credentials=EnvVar("GCP_CREDS")
    )
}
