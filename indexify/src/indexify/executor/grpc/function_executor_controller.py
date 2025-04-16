import asyncio
from typing import Any, Optional

from tensorlake.function_executor.proto.function_executor_pb2 import (
    InitializeRequest,
    SerializedObject,
)
from tensorlake.function_executor.proto.message_validator import MessageValidator

from indexify.proto.executor_api_pb2 import (
    FunctionExecutorDescription,
    FunctionExecutorResources,
)
from indexify.proto.executor_api_pb2 import (
    FunctionExecutorStatus as FunctionExecutorStatusProto,
)

from ..downloader import Downloader
from ..function_executor.function_executor import CustomerError, FunctionExecutor
from ..function_executor.function_executor_state import FunctionExecutorState
from ..function_executor.function_executor_status import FunctionExecutorStatus
from ..function_executor.health_checker import HealthCheckResult
from ..function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerConfiguration,
    FunctionExecutorServerFactory,
)


def validate_function_executor_description(
    function_executor_description: FunctionExecutorDescription,
) -> None:
    """Validates the supplied FE description.

    Raises ValueError if the description is not valid.
    """
    validator = MessageValidator(function_executor_description)
    validator.required_field("id")
    validator.required_field("namespace")
    validator.required_field("graph_name")
    validator.required_field("graph_version")
    validator.required_field("function_name")
    # TODO: Make graph required after we migrate to direct S3 downloads.
    # image_uri is optional.
    # secret_names can be empty.
    # resource_limits is optional.
    # TODO: Make resources required after we migrate Server to them.
    # validator.required_field("resources")
    # validator = MessageValidator(function_executor_description.resources)
    # validator.required_field("cpu_ms_per_sec")
    # validator.required_field("memory_bytes")
    # validator.required_field("disk_bytes")
    # validator.required_field("gpu_count")


def function_executor_logger(
    function_executor_description: FunctionExecutorDescription, logger: Any
) -> Any:
    """Returns a logger bound with the FE's metadata.

    The function assumes that the FE might be invalid."""
    return logger.bind(
        function_executor_id=(
            function_executor_description.id
            if function_executor_description.HasField("id")
            else None
        ),
        namespace=(
            function_executor_description.namespace
            if function_executor_description.HasField("namespace")
            else None
        ),
        graph_name=(
            function_executor_description.graph_name
            if function_executor_description.HasField("graph_name")
            else None
        ),
        graph_version=(
            function_executor_description.graph_version
            if function_executor_description.HasField("graph_version")
            else None
        ),
        function_name=(
            function_executor_description.function_name
            if function_executor_description.HasField("function_name")
            else None
        ),
    )


class FunctionExecutorController:
    def __init__(
        self,
        executor_id: str,
        function_executor_state: FunctionExecutorState,
        function_executor_description: FunctionExecutorDescription,
        function_executor_server_factory: FunctionExecutorServerFactory,
        downloader: Downloader,
        base_url: str,
        config_path: str,
        logger: Any,
    ):
        """Initializes the FunctionExecutorController.

        The supplied FunctionExecutorDescription must be already validated by the caller
        using validate_function_executor_description().
        """
        self._executor_id: str = executor_id
        self._function_executor_state: FunctionExecutorState = function_executor_state
        self._function_executor_description: FunctionExecutorDescription = (
            function_executor_description
        )
        self._function_executor_server_factory: FunctionExecutorServerFactory = (
            function_executor_server_factory
        )
        self._downloader: Downloader = downloader
        self._base_url: str = base_url
        self._config_path: str = config_path
        self._logger: Any = function_executor_logger(
            function_executor_description, logger
        ).bind(
            module=__name__,
        )
        # The locks protects the desired status.
        self._lock: asyncio.Lock = asyncio.Lock()
        # The same as the initial FE status.
        self._desired_status: FunctionExecutorStatusProto = (
            FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_STOPPED
        )
        self._desired_status_change_notifier: asyncio.Condition = asyncio.Condition(
            lock=self._lock
        )
        # Automatically start the controller on creation.
        self._reconciliation_loop_task: asyncio.Task = asyncio.create_task(
            self._reconciliation_loop(),
            name="function executor controller reconciliation loop",
        )

    def function_executor_description(self) -> FunctionExecutorDescription:
        return self._function_executor_description

    async def startup(self) -> None:
        await self._set_desired_status(
            FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_IDLE
        )

    async def shutdown(self) -> None:
        await self._set_desired_status(
            FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_SHUTDOWN
        )

    async def _set_desired_status(
        self, desired_status: FunctionExecutorStatusProto
    ) -> None:
        """Updates the desired Function Executor status.

        Reconciliation is done asynchronously. Doesn't raise any exceptions.
        """
        async with self._lock:
            if self._desired_status == desired_status:
                return
            self._desired_status = desired_status
            self._desired_status_change_notifier.notify_all()

    async def _reconciliation_loop(self) -> None:
        self._logger.info("function executor controller reconciliation loop started")
        # The same as the initial FE status.
        last_seen_desired_status: FunctionExecutorStatusProto = (
            FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_STOPPED
        )
        # The loop is exited via loop async task cancellation on FE shutdown.
        while True:
            async with self._lock:
                while last_seen_desired_status == self._desired_status:
                    await self._desired_status_change_notifier.wait()

            last_seen_desired_status = self._desired_status
            # It's guaranteed that we don't run _reconcile concurrently multiple times.
            await self._reconcile(last_seen_desired_status)

    async def _reconcile(self, desired_status: FunctionExecutorStatusProto) -> None:
        """Reconciles the FE status with the desired status.

        Doesn't raise any exceptions."""
        async with self._function_executor_state.lock:
            if (
                desired_status
                == FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_IDLE
            ):
                return await self._startup()
            elif (
                desired_status
                == FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_SHUTDOWN
            ):
                # Shutdown can be requested with any current status.
                return await self._shutdown()
            else:
                self._logger.error(
                    "unexpected desired function executor status received from server, skipping state reconciliation",
                    current_status=self._function_executor_state.status.name,
                    desired_status=FunctionExecutorStatusProto.Name(desired_status),
                )

    async def _shutdown(self) -> None:
        """Shutsdown the Function Executor and frees all of its resources.

        Caller holds the FE state lock. Doesn't raise any exceptions.
        """
        # Run destroy sequence if current FE status requires it (see allows FE status transitions).
        # We won't see DESTROYING and STARTING_UP statuses here because FE reconciliation is done
        # with concurrency of 1.
        if self._function_executor_state.status in [
            FunctionExecutorStatus.STARTUP_FAILED_PLATFORM_ERROR,
            FunctionExecutorStatus.STARTUP_FAILED_CUSTOMER_ERROR,
            FunctionExecutorStatus.IDLE,
            FunctionExecutorStatus.RUNNING_TASK,
            FunctionExecutorStatus.UNHEALTHY,
        ]:
            await self._function_executor_state.set_status(
                FunctionExecutorStatus.DESTROYING
            )
            if self._function_executor_state.function_executor is not None:
                async with _UnlockedLockContextManager(
                    self._function_executor_state.lock
                ):
                    await self._function_executor_state.function_executor.destroy()
            await self._function_executor_state.set_status(
                FunctionExecutorStatus.DESTROYED
            )
            self._function_executor_state.function_executor = None

        self._logger.info("shutting down function executor controller")
        await self._function_executor_state.set_status(FunctionExecutorStatus.SHUTDOWN)
        self._reconciliation_loop_task.cancel()
        # No code is executed after this point because reconciliation loop aio task is cancelled.

    async def _startup(self) -> None:
        """Startups the FE if possible.

        Caller holds the FE state lock. Doesn't raise any exceptions.
        """
        if self._function_executor_state.status != FunctionExecutorStatus.DESTROYED:
            self._logger.error(
                "Can't startup Function Executor from its current state, skipping startup",
                current_status=self._function_executor_state.status.name,
            )
            return

        await self._function_executor_state.set_status(
            FunctionExecutorStatus.STARTING_UP
        )

        next_status: FunctionExecutorStatus = FunctionExecutorStatus.IDLE
        next_status_message: str = ""
        async with _UnlockedLockContextManager(self._function_executor_state.lock):
            try:
                function_executor: FunctionExecutor = await _create_function_executor(
                    function_executor_description=self._function_executor_description,
                    function_executor_server_factory=self._function_executor_server_factory,
                    downloader=self._downloader,
                    executor_id=self._executor_id,
                    base_url=self._base_url,
                    config_path=self._config_path,
                    logger=self._logger,
                )
            except CustomerError as e:
                next_status = FunctionExecutorStatus.STARTUP_FAILED_CUSTOMER_ERROR
                next_status_message = str(e)
            except Exception as e:
                next_status = FunctionExecutorStatus.STARTUP_FAILED_PLATFORM_ERROR
                self._logger.error("failed to create function executor", exc_info=e)

        # FE state lock is acquired again at this point.
        await self._function_executor_state.set_status(next_status, next_status_message)

        if next_status == FunctionExecutorStatus.IDLE:
            # Task controllers will notice that this FE is IDLE and start running on it one by one.
            self._function_executor_state.function_executor = function_executor
            # Health checker starts after FE creation and gets automatically stopped on FE destroy.
            self._function_executor_state.function_executor.health_checker().start(
                self._health_check_failed_callback
            )

    async def _health_check_failed_callback(self, result: HealthCheckResult):
        async with self._function_executor_state.lock:
            if self._function_executor_state.status == FunctionExecutorStatus.UNHEALTHY:
                return

            # There can be false positive health check failures when we're creating
            # or destroying FEs so we only react to health check failures when we expect
            # the FE to be healthy.
            if self._function_executor_state.status not in (
                FunctionExecutorStatus.IDLE,
                FunctionExecutorStatus.RUNNING_TASK,
            ):
                return

            await self._function_executor_state.set_status(
                FunctionExecutorStatus.UNHEALTHY
            )
            function_executor: FunctionExecutor = (
                self._function_executor_state.function_executor
            )
            self._function_executor_state.function_executor = None

        self._logger.error(
            "Function Executor health check failed, destroying Function Executor",
            health_check_fail_reason=result.reason,
        )
        # Destroy the unhealthy FE asap so it doesn't consume resources.
        # Do it with unlocked state lock to not stop other work on this FE state.
        await function_executor.destroy()


async def _create_function_executor(
    function_executor_description: FunctionExecutorDescription,
    function_executor_server_factory: FunctionExecutorServerFactory,
    downloader: Downloader,
    executor_id: str,
    base_url: str,
    config_path: str,
    logger: Any,
) -> FunctionExecutor:
    """Creates a function executor.

    Raises Exception in case of failure.
    Raises CustomerError if customer code failed during FE creation.
    """
    graph: SerializedObject = await downloader.download_graph(
        namespace=function_executor_description.namespace,
        graph_name=function_executor_description.graph_name,
        graph_version=function_executor_description.graph_version,
        logger=logger,
        data_payload=(
            function_executor_description.graph
            if function_executor_description.HasField("graph")
            else None
        ),
    )

    config: FunctionExecutorServerConfiguration = FunctionExecutorServerConfiguration(
        executor_id=executor_id,
        function_executor_id=function_executor_description.id,
        namespace=function_executor_description.namespace,
        graph_name=function_executor_description.graph_name,
        graph_version=function_executor_description.graph_version,
        function_name=function_executor_description.function_name,
        image_uri=None,
        secret_names=list(function_executor_description.secret_names),
        cpu_ms_per_sec=None,
        memory_bytes=None,
        disk_bytes=None,
        gpu_count=0,
    )
    if function_executor_description.HasField("image_uri"):
        config.image_uri = function_executor_description.image_uri
    if function_executor_description.HasField("resources"):
        resources: FunctionExecutorResources = function_executor_description.resources
        config.cpu_ms_per_sec = resources.cpu_ms_per_sec
        config.memory_bytes = resources.memory_bytes
        config.disk_bytes = resources.disk_bytes
        config.gpu_count = resources.gpu_count

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
        # Raises CustomerError if initialization failed in customer code or customer code timed out.
        await function_executor.initialize(
            config=config,
            initialize_request=initialize_request,
            base_url=base_url,
            config_path=config_path,
            customer_code_timeout_sec=customer_code_timeout_sec,
        )
        return function_executor
    except (Exception, asyncio.CancelledError):
        # Destroy the failed to startup FE asap so it doesn't consume resources.
        # Destroy the FE also if the FE initialization got cancelled to not leak
        # allocated resources.
        await function_executor.destroy()
        raise


class _UnlockedLockContextManager:
    """Unlocks its lock on enter to the scope and locks it back on exit."""

    def __init__(
        self,
        lock: asyncio.Lock,
    ):
        self._lock: asyncio.Lock = lock

    async def __aenter__(self):
        self._lock.release()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._lock.acquire()
