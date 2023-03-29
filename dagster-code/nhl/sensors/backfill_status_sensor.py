from dagster import (
    sensor,
    SensorEvaluationContext,
    EventRecordsFilter,
    BulkActionStatus,
)


@sensor(
    minimum_interval_seconds=5,
    description="checks for backfills and writes out the status to slack",
    required_resource_keys="coc_slack_resource",
)
def backfill_status_sensor(context: SensorEvaluationContext):
    cursor_dict = json.loads(context.cursor) if context.cursor else {}
    last_backfill_sensor_cursor = cursor_dict.get("backfill")

    all_backfills = context.instance.get_backfills()
    statuses = {b.backfill_id: b.status.value for b in all_backfills}

    context.update_cursor(json.dumps({"backfill": backfills[0].storage_id}))
