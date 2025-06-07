import os
from typing import Any, List

from .events import BaseEvent

_DEBUG_EVENT_LOOP: bool = (
    os.getenv("INDEXIFY_FUNCTION_EXECUTOR_CONTROLLER_DEBUG_EVENT_LOOP", "0")
) == "1"


def debug_print_processing_event(event: BaseEvent, logger: Any) -> None:
    if _DEBUG_EVENT_LOOP:
        logger.debug(
            "processing event in control loop",
            fe_event=str(event),
        )


def debug_print_adding_event(event: BaseEvent, source: str, logger: Any) -> None:
    if _DEBUG_EVENT_LOOP:
        logger.debug(
            "adding event to control loop",
            source=source,
            fe_event=str(event),
        )


def debug_print_events(events: List[BaseEvent], logger: Any) -> None:
    if _DEBUG_EVENT_LOOP:
        if len(events) == 0:
            logger.debug("no events n control loop")
        else:
            logger.debug(
                "events in control loop",
                count=len(events),
                fe_events=[str(event) for event in events],
            )
