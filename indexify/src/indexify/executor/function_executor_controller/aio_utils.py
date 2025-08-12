import asyncio
from typing import Any, Optional


async def shielded_await(task: asyncio.Task, logger: Any) -> Any:
    """Awaits the supplied task and ignores cancellations until it's done.

    Cancels itself if the task is cancelled once the task is done.
    """
    cancelled_error: Optional[asyncio.CancelledError] = None

    while not task.done():
        try:
            # Shield to make sure that task is not cancelled.
            await asyncio.shield(task)
        except asyncio.CancelledError as e:
            logger.info(
                "ignoring aio task cancellation until it's finished",
                task_name=task.get_name(),
            )
            cancelled_error = e

    if cancelled_error is not None:
        raise cancelled_error
    else:
        return task.result()
