import asyncio
from typing import Any, Optional

from tensorlake.function_executor.proto.function_executor_pb2 import (
    InitializeRequest,
    SerializedObject,
)
from tensorlake.function_executor.proto.message_validator import MessageValidator

from indexify.proto.executor_api_pb2 import (
    FunctionExecutorDescription,
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

        Raises ValueError if the supplied FunctionExecutorDescription is not valid.
        """
        _validate_function_executor_description(function_executor_description)
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
        self._logger: Any = logger.bind(
            module=__name__,
            function_executor_id=function_executor_description.id,
            namespace=function_executor_description.namespace,
            graph_name=function_executor_description.graph_name,
            graph_version=function_executor_description.graph_version,
            function_name=function_executor_description.function_name,
            image_uri=function_executor_description.image_uri,
        )
        self._reconciliation_loop_task: asyncio.Task = asyncio.create_task(
            self._reconciliation_loop()
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

    async def set_desired_status(
        self, desired_status: FunctionExecutorStatusProto
    ) -> None:
        """Updates the desired Function Executor status.

        Reconciliation is done asynchronously.
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
        async with self._function_executor_state.lock:
            current_status: FunctionExecutorStatus = (
                self._function_executor_state.status
            )
            # We have to process all possible combination of current and desired statuses.
            if current_status == FunctionExecutorStatus.STARTUP_FAILED_CUSTOMER_ERROR:
                if (
                    desired_status
                    == FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_CUSTOMER_ERROR
                ):
                    return  # Same status, nothing to do.

                # All we can do from the current status is to destroy the FE to possibly recreate it later
                # if Server requests to do this. This is why we don't accept any other desired statuses.
                return await self._destroy_or_shutdown_fe_if_desired(desired_status)

            if current_status == FunctionExecutorStatus.STARTUP_FAILED_PLATFORM_ERROR:
                if (
                    desired_status
                    == FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_PLATFORM_ERROR
                ):
                    return  # Same status, nothing to do.

                # All we can do from the current status is to destroy the FE to possibly recreate it later
                # if Server requests to do this. This is why we don't accept any other desired statuses.
                return await self._destroy_or_shutdown_fe_if_desired(desired_status)

            if current_status == FunctionExecutorStatus.IDLE:
                if (
                    desired_status
                    == FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_IDLE
                ):
                    return  # Same status, nothing to do.

                # Server can only request FE destroy or shutdown when FE has IDLE status.
                # Transition from IDLE to RUNNING_TASK can only be done by Task controller.
                # Transition from IDLE to UNHEALTHY can only be done by FE controller.
                return await self._destroy_or_shutdown_fe_if_desired(desired_status)

            if current_status == FunctionExecutorStatus.RUNNING_TASK:
                if (
                    desired_status
                    == FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_RUNNING_TASK
                ):
                    return  # Same status, nothing to do.

                # Server can only request FE destroy or shutdown when FE has RUNNING_TASK status.
                # Transition from RUNNING_TASK to UNHEALTHY can only be done by Task controller.
                return await self._destroy_or_shutdown_fe_if_desired(desired_status)

            if current_status == FunctionExecutorStatus.UNHEALTHY:
                if (
                    desired_status
                    == FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_UNHEALTHY
                ):
                    return  # Same status, nothing to do.

                # Server can only request FE destroy or shutdown when FE has RUNNING_TASK status.
                return await self._destroy_or_shutdown_fe_if_desired(desired_status)

            if current_status == FunctionExecutorStatus.DESTROYED:
                if (
                    desired_status
                    == FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_STOPPED
                ):
                    return  # Same status, nothing to do.

                return await self._reconcile_from_destroyed(desired_status)

            # _reconcile() can't be called when current FE status is one of "long running" states
            # handled by FE controller like STARTING_UP and DESTROYING. This is because _reconcile()
            # is called with concurrency of 1 and _reconcile() waits until these long running states
            # (operations) are finished before returning.
            #
            # It's not possible to have SHUTDOWN current status because when FE controller transitions to SHUTDOWN
            # status, it cancels the reconciliation loop task.
            self._logger.error(
                "unexpected current function executor status, skipping state reconciliation",
                current_status=current_status.name,
                desired_status=FunctionExecutorStatusProto.Name(desired_status),
            )

    async def _destroy_or_shutdown_fe_if_desired(
        self, desired_status: FunctionExecutorStatusProto
    ) -> None:
        """Destroys the Function Executor if desired status asks for it.

        Otherwise logs an error because other actions are not allowed by the current status.
        Caller holds the FE state lock.
        """
        if desired_status not in [
            FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_STOPPING,
            FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_STOPPED,
            FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_SHUTDOWN,
        ]:
            self._logger.error(
                "unexpected desired function executor status received from server, skipping state reconciliation",
                current_status=self._function_executor_state.status.name,
                desired_status=FunctionExecutorStatusProto.Name(desired_status),
            )
            return

        await self._destroy_function_executor()
        # FE state status is now DESTROYED.
        if (
            desired_status
            == FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_SHUTDOWN
        ):
            await self._shutdown()
            # No code is executed after this point because reconciliation loop aio task is cancelled.

    async def _reconcile_from_destroyed(
        self, desired_status: FunctionExecutorStatusProto
    ) -> None:
        """Reconciles the FE state when it has DESTROYED status.

        Caller holds the FE state lock.
        """
        if desired_status not in [
            FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_STARTING_UP,
            FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_IDLE,
            FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_RUNNING_TASK,
            FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_SHUTDOWN,
        ]:
            self._logger.error(
                "unexpected desired function executor status received from server, skipping state reconciliation",
                current_status=self._function_executor_state.status.name,
                desired_status=FunctionExecutorStatusProto.Name(desired_status),
            )
            return

        if (
            desired_status
            == FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_SHUTDOWN
        ):
            await self._shutdown()
            # No code is executed after this point because reconciliation loop aio task is cancelled.
            return

        # All the rest of the allowed desired statuses ask to create the FE.
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

        # FE state lock is acquired again at this point.
        await self._function_executor_state.set_status(next_status, next_status_message)

        if next_status == FunctionExecutorStatus.IDLE:
            # Task controllers will notice that this FE is IDLE and start running on it one by one.
            self._function_executor_state.function_executor = function_executor
            # Health checker starts after FE creation and gets automatically stopped on FE destroy.
            self._function_executor_state.function_executor.health_checker().start(
                self._health_check_failed_callback
            )

    async def _destroy_function_executor(self) -> None:
        """Destroys the Function Executor if it exists.

        Caller holds the FE state lock.
        """
        await self._function_executor_state.set_status(
            FunctionExecutorStatus.DESTROYING
        )
        async with _UnlockedLockContextManager(self._function_executor_state.lock):
            await self._function_executor_state.function_executor.destroy()
        await self._function_executor_state.set_status(FunctionExecutorStatus.DESTROYED)
        self._function_executor_state.function_executor = None

    async def _shutdown(self) -> None:
        """Shuts down the controller.

        Caller holds the FE state lock.
        Raises asyncio.CancelledError on return when called from reconciliation loop.
        """
        self._logger.info("shutting down function executor controller")
        await self._function_executor_state.set_status(FunctionExecutorStatus.SHUTDOWN)
        self._reconciliation_loop_task.cancel()
        await self._reconciliation_loop_task

    async def _health_check_failed_callback(self, result: HealthCheckResult):
        async with self._function_executor_state.lock:
            if self._function_executor_state.status == FunctionExecutorStatus.UNHEALTHY:
                return

            if self._function_executor_state.status in (
                FunctionExecutorStatus.IDLE,
                FunctionExecutorStatus.RUNNING_TASK,
            ):
                # There can be false positive health check failures when we're creating
                # or destroying FEs so we're not interested in them.
                #
                # Server should react to this transition into unhealthy state and ask to
                # destroy this FE.
                await self._function_executor_state.set_status(
                    FunctionExecutorStatus.UNHEALTHY
                )


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
    )

    config: FunctionExecutorServerConfiguration = FunctionExecutorServerConfiguration(
        executor_id=executor_id,
        function_executor_id=function_executor_description.id,
        namespace=function_executor_description.namespace,
        secret_names=list(function_executor_description.secret_names),
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
        # TODO: Add integration tests with FE customer code initialization timeout
        # when end-to-end implementation is done.
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
    except Exception:
        await function_executor.destroy()
        raise


def _validate_function_executor_description(
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
    # image_uri is optional.
    # secret_names can be empty.
    # resource_limits is optional.


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
