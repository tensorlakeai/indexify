import os
from typing import Any

import httpx
from pydantic import BaseModel

from .metrics.executor import (
    metric_executor_event_push_errors,
    metric_executor_events_pushed,
)

ENVIRONMENT_EVENT_COLLECTOR_URL = os.environ.get("TENSORLAKE_EVENT_COLLECTOR_URL")


class Resource(BaseModel):
    namespace: str
    application: str
    application_version: str
    executor_id: str
    fn_executor_id: str
    fn: str


class EventCollector:
    def __init__(self, collector_url: str | None = ENVIRONMENT_EVENT_COLLECTOR_URL):
        self._collector_url: str = collector_url
        self._client = httpx.Client()

    def push_event(
        self, resource: Resource, event: dict[str, Any], logger: Any
    ) -> None:
        """
        Pushes the given event to a log collector.
        If the collector_url is not set, the event is ignored.

        This function does not capture any exceptions that may occur during the HTTP request
        because it's designed to be embedded into the executor.
        The executor needs to handle HTTP errors and collect metrics.
        """
        if self._collector_url:
            logger: Any = logger.bind(module=__name__)
            try:
                metric_executor_events_pushed.inc()

                body = resource.model_dump()
                body["event"] = event

                response = self._client.post(self._collector_url, json=body)
                _ = response.raise_for_status()
            except Exception as e:
                metric_executor_event_push_errors.inc()
                logger.error("Failed to push event to collector", exc_info=e)
