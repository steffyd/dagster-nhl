from dagster._utils import file_relative_path

DBT_PROJECT_DIR = file_relative_path(__file__, "../nhl_dbt")
DBT_PROFILES_DIR = DBT_PROJECT_DIR + "/config"
