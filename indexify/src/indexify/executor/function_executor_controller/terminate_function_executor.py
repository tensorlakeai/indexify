import asyncio
from typing import Any, List, Optional

from indexify.executor.function_executor.function_executor import FunctionExecutor
from indexify.proto.executor_api_pb2 import FunctionExecutorTerminationReason

from .aio_utils import shielded_await
from .events import FunctionExecutorTerminated


async def terminate_function_executor(
    function_executor: Optional[FunctionExecutor],
    lock: asyncio.Lock,
    fe_termination_reason: FunctionExecutorTerminationReason,
    allocation_ids_caused_termination: List[str],
    logger: Any,
) -> FunctionExecutorTerminated:
    """Destroys the function executor if it's not None.

    The supplied lock is used to ensure that if a destroy operation is in progress,
    then another caller won't return immediately assuming that the destroy is complete
    due to its idempotency. Ignores cancellations while destroying the function executor.

    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)

    if function_executor is not None:
        async with lock:
            logger.info(
                "destroying function executor",
            )
            fe_destroy_task: asyncio.Task = asyncio.create_task(
                function_executor.destroy(),
                name="destroy function executor",
            )
            try:
                await shielded_await(fe_destroy_task, logger)
            except asyncio.CancelledError:
                # We actually destroyed the FE so we can return without error.
                pass

    return FunctionExecutorTerminated(
        is_success=True,
        fe_termination_reason=fe_termination_reason,
        allocation_ids_caused_termination=allocation_ids_caused_termination,
    )
