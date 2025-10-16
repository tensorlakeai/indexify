import os
from typing import Any

import httpx
from pydantic import BaseModel

from .metrics.executor import (
    metric_executor_event_push_errors,
    metric_executor_events_pushed,
)


class Resource(BaseModel):
    namespace: str
    application: str
    application_version: str
    executor_id: str
    fn_executor_id: str
    fn: str


def push_event_to_collector(
    resource: Resource,
    event: dict[str, Any],
    logger: Any,
    collector_url: str | None = None,
) -> None:
    """
    Pushes the given event to a log collector.
    This function relies on the existence of the TENSORLAKE_EVENT_COLLECTOR_URL environment variable.
    If the environment variable is not set, the event is ignored.

    This function does not capture any exceptions that may occur during the HTTP request
    because it's designed to be embedded into the executor.
    The executor needs to handle HTTP errors and collect metrics.
    """
    collector_url = (
        os.environ.get("TENSORLAKE_EVENT_COLLECTOR_URL")
        if collector_url is None
        else collector_url
    )

    if collector_url:
        try:
            metric_executor_events_pushed.inc()

            body = resource.model_dump()
            body["event"] = event

            response = httpx.post(collector_url, json=body)
            response.raise_for_status()
        except Exception as error:
            metric_executor_event_push_errors.inc()
            logger.error("Failed to push event to collector", error)
