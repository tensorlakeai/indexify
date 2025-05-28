from typing import Any, Optional

from indexify.executor.function_executor.function_executor import FunctionExecutor

from .events import FunctionExecutorDestroyed


async def destroy_function_executor(
    function_executor: Optional[FunctionExecutor],
    logger: Any,
) -> FunctionExecutorDestroyed:
    """Destroys a function executor.

    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)

    if function_executor is not None:
        logger.info(
            "destroying function executor",
        )
        await function_executor.destroy()

    return FunctionExecutorDestroyed(is_success=True)
