import asyncio
from pathlib import Path
from typing import Any, Tuple

from tensorlake.function_executor.proto.function_executor_pb2 import (
    FunctionRef,
    InitializationFailureReason,
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
    SerializedObject,
)
from tensorlake.function_executor.proto.message_validator import MessageValidator

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.executor.function_executor.function_executor import (
    FunctionExecutor,
    FunctionExecutorInitializationResult,
)
from indexify.executor.function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerConfiguration,
    FunctionExecutorServerFactory,
)
from indexify.proto.executor_api_pb2 import (
    FunctionExecutorDescription,
    FunctionExecutorTerminationReason,
)

from .aio_utils import shielded_await
from .downloads import download_application_code
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
    except asyncio.CancelledError:
        # Cancelled FE startup means that Server removed this FE from desired state. We don't have FE termination reason for the case
        # when Server removed FE from desired state because we can't rely on its delivery because FE removed from desired state can get
        # removed from reported state at any moment. Thus we can use any termination reason here.
        return FunctionExecutorCreated(
            function_executor=None,
            fe_termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_CANCELLED,
        )
    except BaseException as e:
        logger.error(
            "failed to create function executor",
            exc_info=e,
        )
        return FunctionExecutorCreated(
            function_executor=None,
            fe_termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_INTERNAL_ERROR,
        )

    function_executor: FunctionExecutor
    result: FunctionExecutorInitializationResult
    # No await here so this call can't be cancelled.
    fe_created_event: FunctionExecutorCreated = _to_fe_created_event(
        function_executor=function_executor,
        result=result,
        logger=logger,
    )
    if fe_created_event.function_executor is None:
        # _to_fe_created_event doesn't like the FE, destroy it.
        fe_destroy_task: asyncio.Task = asyncio.create_task(
            function_executor.destroy(),
            name=f"destroy function executor {function_executor_description.id}",
        )
        try:
            await shielded_await(fe_destroy_task, logger)
        except asyncio.CancelledError:
            # destroy() finished due to the shield, return fe_created_event.
            pass

    return fe_created_event


def _to_fe_created_event(
    function_executor: FunctionExecutor,
    result: FunctionExecutorInitializationResult,
    logger: Any,
) -> FunctionExecutorCreated:
    """Converts FunctionExecutorInitializationResult to FunctionExecutorCreated event.

    Doesn't raise any exceptions.
    """
    if result.is_timeout:
        return FunctionExecutorCreated(
            function_executor=None,
            fe_termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT,
        )

    if result.response is None:
        # This is a grey failure where we don't know the exact cause.
        # Treat it as a customer function error to prevent service abuse by intentionally
        # triggering function executor creations failures that don't get billed.
        logger.error("function executor startup failed with no response")
        return FunctionExecutorCreated(
            function_executor=None,
            fe_termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR,
        )

    initialize_response: InitializeResponse = result.response
    try:
        _validate_initialize_response(initialize_response)
    except ValueError as e:
        # Grey failure mode. Treat as customer function error to prevent service abuse but log for future investigations.
        logger.error(
            "function executor initialization failed with invalid response", exc_info=e
        )
        return FunctionExecutorCreated(
            function_executor=None,
            fe_termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR,
        )

    if (
        initialize_response.outcome_code
        == InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS
    ):
        return FunctionExecutorCreated(
            function_executor=function_executor,
            fe_termination_reason=None,
        )
    else:
        if (
            initialize_response.failure_reason
            == InitializationFailureReason.INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR
        ):
            return FunctionExecutorCreated(
                function_executor=None,
                fe_termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR,
            )
        else:
            # Treat all other failure reasons as grey failures. Report them as function errors to prevent service abuse.
            # Log them for awareness and future investigations.
            logger.error(
                "function executor initialization failed",
                failure_reason=InitializationFailureReason.Name(
                    initialize_response.failure_reason
                ),
            )
            return FunctionExecutorCreated(
                function_executor=None,
                fe_termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR,
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
) -> Tuple[FunctionExecutor, FunctionExecutorInitializationResult]:
    """Creates a function executor.

    Raises Exception on internal Executor error.
    """
    application_code: SerializedObject = await download_application_code(
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
        namespace=function_executor_description.function.namespace,
        application_name=function_executor_description.function.application_name,
        application_version=function_executor_description.function.application_version,
        function_name=function_executor_description.function.function_name,
        secret_names=list(function_executor_description.secret_names),
        cpu_ms_per_sec=function_executor_description.resources.cpu_ms_per_sec,
        memory_bytes=function_executor_description.resources.memory_bytes,
        disk_bytes=function_executor_description.resources.disk_bytes,
        gpu_count=gpu_count,
    )

    initialize_request: InitializeRequest = InitializeRequest(
        function=FunctionRef(
            namespace=function_executor_description.function.namespace,
            application_name=function_executor_description.function.application_name,
            application_version=function_executor_description.function.application_version,
            function_name=function_executor_description.function.function_name,
        ),
        application_code=application_code,
    )
    customer_code_timeout_sec: float = (
        function_executor_description.initialization_timeout_ms / 1000.0
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
    except BaseException:
        fe_destroy_task: asyncio.Task = asyncio.create_task(
            function_executor.destroy(),
            name=f"destroy function executor {function_executor_description.id}",
        )
        # This await is a cancellation point, need to shield to ensure we destroyed the FE.
        await shielded_await(
            fe_destroy_task,
            logger,
        )
        raise


def _validate_initialize_response(
    response: InitializeResponse,
) -> None:
    """Validates the initialization response.

    Raises ValueError if the response is not valid.
    """
    validator: MessageValidator = MessageValidator(response)
    (validator.required_field("outcome_code"))
    if (
        response.outcome_code
        == InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_FAILURE
    ):
        validator.required_field("failure_reason")

    if response.outcome_code not in [
        InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
        InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_FAILURE,
    ]:
        raise ValueError(f"Invalid outcome code: {response.outcome_code}")

    if response.failure_reason not in [
        InitializationFailureReason.INITIALIZATION_FAILURE_REASON_UNKNOWN,
        InitializationFailureReason.INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR,
        InitializationFailureReason.INITIALIZATION_FAILURE_REASON_INTERNAL_ERROR,
    ]:
        raise ValueError(f"Invalid failure reason: {response.failure_reason}")
