from .postgres_resource import postgres_resource_by_db
from .postgres_partitioned_io_manager import postgres_partitioned_io_manager
from .coc_slack_resource import coc_slack_resource
from dagster import EnvVar
from utils.constants import GCP_PROJECT_ID
from dagster_gcp import BigQueryResource, GCSResource
from .espn_api_resource import EspnApiResource

RESOURCES = {
    "postgres_resource_by_db": postgres_resource_by_db,
    "postgres_partitioned_io_manager": postgres_partitioned_io_manager,
    "coc_slack_resource": coc_slack_resource,
    "bigquery": BigQueryResource(project=GCP_PROJECT_ID,
                                 gcp_credentials=EnvVar("GCP_CREDS")),
    "gcs": GCSResource(project=GCP_PROJECT_ID),
    "espn_api": EspnApiResource(),
}
