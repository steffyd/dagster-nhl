from dagster import RunFailureSensorContext
from dagster_slack import make_slack_on_run_failure_sensor
import os


def message_fn(context: RunFailureSensorContext) -> str:
    return (
        f"Job {context.dagster_run.run_id} failed!"
        f"Error: {context.failure_event.message}"
    )


slack_on_run_failure = make_slack_on_run_failure_sensor(
    channel="#dagster-failures",
    slack_token=os.getenv("SLACK_TOKEN"),
    text_fn=message_fn,
    dagit_base_url="https://dagster.corellianengineeringco.com/",
)
