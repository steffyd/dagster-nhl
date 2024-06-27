from dagster import ConfigurableIOManager
from dagster_gcp import BigQueryResource
from google.cloud import bigquery

class SeasonPartitionedBigQueryIOManager(ConfigurableIOManager):
    bq_resource: BigQueryResource

    def load_input(self, context):
        with self.bq_resource.get_client() as client:
            # client is a google.cloud.bigquery.Client
            # table name is {asset_key}_{partition_key}
            table_id = f"{'_'.join(context.asset_key.path)}_{context.asset_partition_key}"
            table = client.get_table(table_id)
            rows = client.list_rows(table)
            return [row for row in rows]
            
        

    def handle_output(self, context, obj):
        with self.bq_resource.get_client() as client:
            # client is a google.cloud.bigquery.Client
            # table name is {asset_key}_{partition_key}
            table_id = f"{'_'.join(context.asset_key.path)}_{context.asset_partition_key}"
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition="WRITE_TRUNCATE",
            )
            job = client.load_table_from_json(obj, table_id, job_config=job_config)
            job.result()