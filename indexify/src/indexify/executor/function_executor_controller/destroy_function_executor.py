from typing import Any, Optional

from indexify.executor.function_executor.function_executor import FunctionExecutor
from indexify.proto.executor_api_pb2 import FunctionExecutorTerminationReason

from .events import FunctionExecutorDestroyed


async def destroy_function_executor(
    function_executor: Optional[FunctionExecutor],
    termination_reason: FunctionExecutorTerminationReason,
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

    return FunctionExecutorDestroyed(
        is_success=True, termination_reason=termination_reason
    )
