import asyncio
from typing import Any


async def shielded_await(aio_task: asyncio.Task, logger: Any) -> Any:
    """Awaits the supplied task and ignores cancellations until it's done.

    Cancels itself if the task is cancelled once the task is done.
    """
    cancelled_error: asyncio.CancelledError | None = None

    while not aio_task.done():
        try:
            # Shield to make sure that task is not cancelled.
            await asyncio.shield(aio_task)
        except asyncio.CancelledError as e:
            logger.info(
                "ignoring aio task cancellation until it's finished",
                task_name=aio_task.get_name(),
            )
            cancelled_error = e

    if cancelled_error is not None:
        raise cancelled_error
    else:
        return aio_task.result()
