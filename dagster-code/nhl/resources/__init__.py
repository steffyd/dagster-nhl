from .postgres_resource import postgres_resource_by_db
from .postgres_partitioned_io_manager import postgres_partitioned_io_manager
from .coc_slack_resource import coc_slack_resource

RESOURCES = {
    "postgres_resource_by_db": postgres_resource_by_db,
    "postgres_partitioned_io_manager": postgres_partitioned_io_manager,
    "coc_slack_resource": coc_slack_resource,
}
