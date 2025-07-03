import asyncio
from pathlib import Path
from typing import Any, Optional, Tuple

from tensorlake.function_executor.proto.function_executor_pb2 import (
    InitializeRequest,
    SerializedObject,
)

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.executor.function_executor.function_executor import (
    FunctionExecutor,
    FunctionExecutorInitializationError,
    FunctionExecutorInitializationResult,
)
from indexify.executor.function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerConfiguration,
    FunctionExecutorServerFactory,
)
from indexify.proto.executor_api_pb2 import (
    DataPayload,
    DataPayloadEncoding,
    FunctionExecutorDescription,
    FunctionExecutorTerminationReason,
)

from .downloads import download_graph
from .events import FunctionExecutorCreated
from .function_executor_startup_output import FunctionExecutorStartupOutput
from .upload_task_output import compute_hash


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
        function_executor, result = await _create_function_executor(
            function_executor_description=function_executor_description,
            function_executor_server_factory=function_executor_server_factory,
            blob_store=blob_store,
            executor_id=executor_id,
            base_url=base_url,
            config_path=config_path,
            cache_path=cache_path,
            logger=logger,
        )
        if result.error is not None:
            await function_executor.destroy()
            function_executor = None

        return FunctionExecutorCreated(
            function_executor=function_executor,
            output=await _initialization_result_to_fe_creation_output(
                function_executor_description=function_executor_description,
                result=result,
                blob_store=blob_store,
                logger=logger,
            ),
        )
    except BaseException as e:
        if isinstance(e, asyncio.CancelledError):
            logger.info("function executor startup was cancelled")
        else:
            logger.error(
                "failed to create function executor due to platform error",
                exc_info=e,
            )

        # Cancelled FE startup means that Server removed it from desired state so it doesn't matter what termination_reason we return
        # in this case cause this FE will be removed from Executor reported state.
        return FunctionExecutorCreated(
            function_executor=None,
            output=FunctionExecutorStartupOutput(
                function_executor_description=function_executor_description,
                termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_INTERNAL_ERROR,
            ),
        )


async def _initialization_result_to_fe_creation_output(
    function_executor_description: FunctionExecutorDescription,
    result: FunctionExecutorInitializationResult,
    blob_store: BLOBStore,
    logger: Any,
) -> FunctionExecutorStartupOutput:
    """Converts FunctionExecutorInitializationResult to FunctionExecutorCreationOutput.

    Uploads stdout and stderr to blob store if they are present. Does only one attempt to do that.
    Doesn't raise any exceptions."""
    termination_reason: FunctionExecutorTerminationReason = None
    if result.error is not None:
        if result.error == FunctionExecutorInitializationError.FUNCTION_ERROR:
            termination_reason = (
                FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR
            )
        elif result.error == FunctionExecutorInitializationError.FUNCTION_TIMEOUT:
            termination_reason = (
                FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT
            )
        else:
            logger.error(
                "unexpected function executor initialization error code",
                error_code=FunctionExecutorInitializationError.name(result.error),
            )
            termination_reason = (
                FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_INTERNAL_ERROR
            )

    stdout: Optional[DataPayload] = None
    if result.stdout is not None:
        url = f"{function_executor_description.output_payload_uri_prefix}/{function_executor_description.id}/stdout"
        stdout = await _upload_initialization_output(
            output_name="stdout",
            output=result.stdout,
            output_url=url,
            blob_store=blob_store,
            logger=logger,
        )

    stderr: Optional[DataPayload] = None
    if result.stderr is not None:
        url = f"{function_executor_description.output_payload_uri_prefix}/{function_executor_description.id}/stderr"
        stderr = await _upload_initialization_output(
            output_name="stderr",
            output=result.stderr,
            output_url=url,
            blob_store=blob_store,
            logger=logger,
        )

    return FunctionExecutorStartupOutput(
        function_executor_description=function_executor_description,
        termination_reason=termination_reason,
        stdout=stdout,
        stderr=stderr,
    )


async def _upload_initialization_output(
    output_name: str, output: str, output_url: str, blob_store: BLOBStore, logger: Any
) -> Optional[DataPayload]:
    """Uploads text to blob store. Returns None if the upload fails.

    Doesn't raise any exceptions.
    """
    try:
        output_bytes: bytes = output.encode()
        await blob_store.put(output_url, output_bytes, logger)
        logger.info(
            f"function executor initialization output {output_name} uploaded to blob store",
            size=len(output_bytes),
        )
        return DataPayload(
            uri=output_url,
            size=len(output_bytes),
            sha256_hash=compute_hash(output_bytes),
            encoding=DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_TEXT,
            encoding_version=0,
        )
    except Exception as e:
        logger.error(
            f"failed to upload function executor initialization output {output_name} to blob store",
            exc_info=e,
        )
        return None


async def _create_function_executor(
    function_executor_description: FunctionExecutorDescription,
    function_executor_server_factory: FunctionExecutorServerFactory,
    blob_store: BLOBStore,
    executor_id: str,
    base_url: str,
    config_path: str,
    cache_path: Path,
    logger: Any,
) -> Tuple[FunctionExecutor, FunctionExecutorInitializationResult]:
    """Creates a function executor.

    Raises Exception on platform error.
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
        result: FunctionExecutorInitializationResult = (
            await function_executor.initialize(
                config=config,
                initialize_request=initialize_request,
                base_url=base_url,
                config_path=config_path,
                customer_code_timeout_sec=customer_code_timeout_sec,
            )
        )
        return (function_executor, result)
    except BaseException:  # includes asyncio.CancelledError and anything else
        await function_executor.destroy()
        raise
