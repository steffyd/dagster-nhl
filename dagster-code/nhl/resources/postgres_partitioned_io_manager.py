import os
import pandas as pd
from dagster import IOManager, io_manager, Partition


class PostgresPartitionedIOManager(IOManager):
    def _get_partition_key(self, context):
        partition = context.asset_partition_key
        if partition:
            return partition
        else:
            raise ValueError("This IO manager requires a partition.")

    def handle_output(self, context, df: pd.DataFrame):
        partition_key = self._get_partition_key(context)
        database = context.metadata["database"]
        table_name = context.metadata["table_name"]
        schema_name = context.metadata["schema_name"]
        if df.empty:
            return

        with context.resources.postgres_resource_by_db(database) as connection:
            with connection.begin() as open_conn:
                try:
                    delete_sql = f"DELETE FROM {schema_name}.{table_name} WHERE partition_key = {partition_key}"
                    open_conn.execute(delete_sql)
                except:
                    # we don't care if this fails, just means that the table doesn't exist yet
                    pass

            # Insert the new data for the partition key
            df["partition_key"] = partition_key
            df.to_sql(
                table_name, connection, schema_name, if_exists="append", index=False
            )

    def load_input(self, context) -> pd.DataFrame:
        partition_key = self._get_partition_key(context)
        database = context.upstream_output.metadata["database"]
        table_name = context.upstream_output.metadata["table_name"]
        schema_name = context.upstream_output.metadata["schema_name"]

        with context.resources.postgres_resource_by_db(database) as connection:
            select_sql = f"SELECT * FROM {schema_name}.{table_name} WHERE partition_key = {partition_key}"

            # Read the data from the table as a Pandas DataFrame
            df = pd.read_sql(select_sql, connection)
            return df


@io_manager(
    required_resource_keys={"postgres_resource_by_db"},
    description="partitioned postgres IO manager, configurable database, table and schema via output metadata",
)
def postgres_partitioned_io_manager():
    return PostgresPartitionedIOManager()
