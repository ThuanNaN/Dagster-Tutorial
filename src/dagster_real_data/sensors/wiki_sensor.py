"""Wikimedia event sensor — triggers materialization on new events."""
import logging

from dagster import sensor, SensorEvaluationContext, RunRequest, SkipReason, DefaultSensorStatus, AssetKey

logger = logging.getLogger(__name__)


@sensor(
    asset_selection=[AssetKey("wiki_events_raw")],
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.STOPPED,
)
def wikimedia_event_sensor(context: SensorEvaluationContext) -> RunRequest | SkipReason:
    wikimedia = context.resources.wikimedia
    cursor = context.cursor

    since_id = cursor.get("last_processed_event_id") if cursor else None
    events = list(wikimedia.stream_events(since_event_id=since_id, max_events=1))

    if not events:
        return SkipReason("No new Wikimedia events detected")

    latest = events[-1]
    cursor_state = wikimedia.get_cursor_state(latest)
    context.update_cursor(cursor_state)

    return RunRequest(
        asset_key=AssetKey("wiki_events_raw"),
        run_key=f"wiki_event_{latest['event_id']}",
    )
