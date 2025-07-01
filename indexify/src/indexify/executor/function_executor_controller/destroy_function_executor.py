import asyncio
from typing import Any, Optional

from indexify.executor.function_executor.function_executor import FunctionExecutor

from .events import FunctionExecutorDestroyed


async def destroy_function_executor(
    function_executor: Optional[FunctionExecutor],
    lock: asyncio.Lock,
    logger: Any,
) -> FunctionExecutorDestroyed:
    """Destroys the function executor if it's not None.

    The supplied lock is used to ensure that if a destroy operation is in progress,
    then another caller won't return immediately assuming that the destroy is complete
    due to its idempotency.

    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)

    if function_executor is not None:
        async with lock:
            logger.info(
                "destroying function executor",
            )
            await function_executor.destroy()

    return FunctionExecutorDestroyed(is_success=True)
