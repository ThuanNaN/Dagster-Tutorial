"""Wikimedia EventStreams resource for SSE ingestion."""
import json
import logging
from typing import Iterator

import requests
from dagster import ConfigurableResource, EnvVar

logger = logging.getLogger(__name__)


class WikimediaResource(ConfigurableResource):
    stream_url: str = EnvVar("WIKIMEDIA_STREAM_URL")
    timeout: int = 30
    max_events: int = 100

    def stream_events(
        self, since_event_id: str | None = None, max_events: int | None = None
    ) -> Iterator[dict]:
        """Stream events from Wikimedia EventStreams via SSE.

        Args:
            since_event_id: Cursor — only return events after this ID.
            max_events: Maximum number of events to yield (bounded).

        Yields:
            Parsed event dictionaries.
        """
        limit = max_events or self.max_events
        count = 0
        params = {}
        if since_event_id:
            params["since_event_id"] = since_event_id

        logger.info("Connecting to Wikimedia stream at %s", self.stream_url)
        response = requests.get(
            self.stream_url,
            stream=True,
            timeout=self.timeout,
            params=params,
        )
        response.raise_for_status()

        for line in response.iter_lines(decode_unicode=True):
            if count >= limit:
                logger.info("Reached max_events limit of %d", limit)
                break

            if not line or line.startswith(":"):
                continue

            if line.startswith("data: "):
                try:
                    event_data = json.loads(line[6:])
                    event = {
                        "event_id": event_data.get("id"),
                        "timestamp": event_data.get("timestamp"),
                        "wiki": event_data.get("wiki", {}).get("database"),
                        "title": event_data.get("title"),
                        "user": event_data.get("user"),
                        "bot": event_data.get("bot", False),
                        "type": event_data.get("type"),
                        "namespace": event_data.get("namespace", {}).get("id"),
                    }
                    count += 1
                    yield event
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning("Skipping malformed event: %s", e)
                    continue

        logger.info("Streamed %d events", count)

    def get_cursor_state(self, event: dict) -> dict:
        """Extract cursor state from an event."""
        return {
            "last_processed_event_id": event.get("event_id"),
            "last_processed_timestamp": event.get("timestamp"),
        }
