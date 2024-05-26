import os
from dagster import sensor, RunRequest, RunConfig, SkipReason
from datetime import datetime as dt


@sensor(required_resource_keys={"gcs"})
def yahoo_nfl_ingestion_from_gcs_sensor(config):
    execution_date = dt.now()
    # get the latest file from the gcs bucket
    latest_file = config.resources.gcs.get_object(
        bucket="yahoo-daily-fantasy-data",
        key=f"nfl/yahoo_nfl_ingestion_{execution_date.strftime('%Y-%m-%d')}.csv",
    )
    # if the latest file is not None, then we have a new file to process
    if latest_file is not None:
        # return a run request for the yahoo_nfl_ingestion_pipeline
        return RunRequest(
            run_key=None,
            run_config=RunConfig(
                tags={
                    "file_name": latest_file["name"],
                    "file_size": latest_file["size"],
                    "file_update_date": latest_file["updated"],
                },
            ),
        )
    # if the latest file is None, then we don't have a new file to process
    else:
        return SkipReason(f"No new files to process for {execution_date.strftime('%Y-%m-%d')}")