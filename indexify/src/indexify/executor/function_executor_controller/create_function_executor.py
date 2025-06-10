import asyncio
from pathlib import Path
from typing import Any, Optional

from tensorlake.function_executor.proto.function_executor_pb2 import (
    InitializeRequest,
    SerializedObject,
)

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.executor.function_executor.function_executor import (
    FunctionError,
    FunctionExecutor,
    FunctionTimeoutError,
)
from indexify.executor.function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerConfiguration,
    FunctionExecutorServerFactory,
)
from indexify.proto.executor_api_pb2 import (
    FunctionExecutorDescription,
    FunctionExecutorTerminationReason,
)

from .downloads import download_graph
from .events import FunctionExecutorCreated


async def create_function_executor(
    function_executor_description: FunctionExecutorDescription,
    function_executor_server_factory: FunctionExecutorServerFactory,
    blob_store: BLOBStore,
    executor_id: str,
    base_url: str,
    config_path: str,
    cache_path: Path,
    logger: Any,
) -> FunctionExecutorCreated:
    """Creates a function executor.

    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)
    try:
        function_executor: FunctionExecutor = await _create_function_executor(
            function_executor_description=function_executor_description,
            function_executor_server_factory=function_executor_server_factory,
            blob_store=blob_store,
            executor_id=executor_id,
            base_url=base_url,
            config_path=config_path,
            cache_path=cache_path,
            logger=logger,
        )
        return FunctionExecutorCreated(function_executor)
    except FunctionTimeoutError as e:
        return FunctionExecutorCreated(
            function_executor=None,
            function_error=e,
            termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT,
        )
    except FunctionError as e:
        return FunctionExecutorCreated(
            function_executor=None,
            function_error=e,
            termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR,
        )
    except BaseException as e:
        if isinstance(e, asyncio.CancelledError):
            logger.info("function executor startup was cancelled")
            return FunctionExecutorCreated(
                function_executor=None,
                termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_REMOVED_FROM_DESIRED_STATE,
            )
        else:
            logger.error(
                "failed to create function executor due to platform error",
                exc_info=e,
            )
            return FunctionExecutorCreated(
                function_executor=None,
                termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_INTERNAL_ERROR,
            )


async def _create_function_executor(
    function_executor_description: FunctionExecutorDescription,
    function_executor_server_factory: FunctionExecutorServerFactory,
    blob_store: BLOBStore,
    executor_id: str,
    base_url: str,
    config_path: str,
    cache_path: Path,
    logger: Any,
) -> FunctionExecutor:
    """Creates a function executor.

    Raises Exception on platform error.
    Raises FunctionError if customer code failed during FE creation.
    """
    graph: SerializedObject = await download_graph(
        function_executor_description=function_executor_description,
        cache_path=cache_path,
        blob_store=blob_store,
        logger=logger,
    )

    gpu_count: int = 0
    if function_executor_description.resources.HasField("gpu"):
        gpu_count = function_executor_description.resources.gpu.count

    config: FunctionExecutorServerConfiguration = FunctionExecutorServerConfiguration(
        executor_id=executor_id,
        function_executor_id=function_executor_description.id,
        namespace=function_executor_description.namespace,
        graph_name=function_executor_description.graph_name,
        graph_version=function_executor_description.graph_version,
        function_name=function_executor_description.function_name,
        image_uri=None,
        secret_names=list(function_executor_description.secret_names),
        cpu_ms_per_sec=function_executor_description.resources.cpu_ms_per_sec,
        memory_bytes=function_executor_description.resources.memory_bytes,
        disk_bytes=function_executor_description.resources.disk_bytes,
        gpu_count=gpu_count,
    )
    if function_executor_description.HasField("image_uri"):
        config.image_uri = function_executor_description.image_uri

    initialize_request: InitializeRequest = InitializeRequest(
        namespace=function_executor_description.namespace,
        graph_name=function_executor_description.graph_name,
        graph_version=function_executor_description.graph_version,
        function_name=function_executor_description.function_name,
        graph=graph,
    )
    customer_code_timeout_sec: Optional[float] = None
    if function_executor_description.HasField("customer_code_timeout_ms"):
        customer_code_timeout_sec = (
            function_executor_description.customer_code_timeout_ms / 1000.0
        )

    function_executor: FunctionExecutor = FunctionExecutor(
        server_factory=function_executor_server_factory, logger=logger
    )

    try:
        # Raises FunctionError if initialization failed in customer code or customer code timed out.
        await function_executor.initialize(
            config=config,
            initialize_request=initialize_request,
            base_url=base_url,
            config_path=config_path,
            customer_code_timeout_sec=customer_code_timeout_sec,
        )
        return function_executor
    except BaseException:  # includes asyncio.CancelledError and anything else
        await function_executor.destroy()
        raise
