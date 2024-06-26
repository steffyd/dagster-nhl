from .postgres_resource import postgres_resource_by_db
from .coc_slack_resource import coc_slack_resource
from dagster import EnvVar
from utils.constants import GCP_PROJECT_ID
from dagster_gcp import BigQueryResource, GCSResource
from .espn_api_resource import EspnApiResource
from .partitioned_gcs_io_manager import PartitionedGCSIOManager
from .season_partitioned_bigquery_io_manager import SeasonPartitionedBigQueryIOManager

gcs_resource = GCSResource(project=GCP_PROJECT_ID)
bq_resource = BigQueryResource(project=GCP_PROJECT_ID, gcp_credentials=EnvVar("GCP_CREDS"))

RESOURCES = {
    "postgres_resource_by_db": postgres_resource_by_db,
    "coc_slack_resource": coc_slack_resource,
    "bigquery": bq_resource,
    "gcs": gcs_resource,
    "espn_api": EspnApiResource(),
    "raw_nhl_data_partitioned_gcs_io_manager": PartitionedGCSIOManager(bucket='dagster-storage-raw-nhl-data', client=gcs_resource),
    "nhl_season_partitioned_bigquery_io_manager": SeasonPartitionedBigQueryIOManager(bq_resource=bq_resource),
}
