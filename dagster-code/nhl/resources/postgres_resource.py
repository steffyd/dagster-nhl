import sqlalchemy as sa
from dagster import resource
import os
from contextlib import contextmanager


database_url = (
    "postgresql://{username}:{password}@{host}:{port}/{database}?sslmode=require"
)


@resource(
    description="configured postgres connection via env vars, with a configurable database set on execution"
)
def postgres_resource_by_db():
    @contextmanager
    def get_postgres_connection(database):
        postgres_conn = sa.create_engine(
            database_url.format(
                username=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                database=database,
            )
        )
        try:
            yield postgres_conn
        finally:
            pass  # dunno clean up some shit I guess

    return get_postgres_connection
