import asyncio
from typing import Any, AsyncGenerator, Dict, Iterable, List, Optional, Set

from tensorlake.function_executor.proto.message_validator import MessageValidator

from indexify.proto.executor_api_pb2 import (
    DesiredExecutorState,
    FunctionExecutorDescription,
    GetDesiredExecutorStatesRequest,
    TaskAllocation,
)
from indexify.proto.executor_api_pb2_grpc import ExecutorAPIStub

from ..downloader import Downloader
from ..function_executor.function_executor_state import FunctionExecutorState
from ..function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from ..function_executor.function_executor_status import FunctionExecutorStatus
from ..function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerFactory,
)
from ..task_reporter import TaskReporter
from .channel_manager import ChannelManager
from .function_executor_controller import (
    FunctionExecutorController,
    function_executor_logger,
    validate_function_executor_description,
)
from .metrics.state_reconciler import (
    metric_state_reconciliation_errors,
    metric_state_reconciliation_latency,
    metric_state_reconciliations,
)
from .state_reporter import ExecutorStateReporter
from .task_controller import TaskController, task_logger, validate_task

_RECONCILE_STREAM_BACKOFF_INTERVAL_SEC = 5
_RECONCILIATION_RETRIES = 3


class ExecutorStateReconciler:
    def __init__(
        self,
        executor_id: str,
        function_executor_server_factory: FunctionExecutorServerFactory,
        base_url: str,
        function_executor_states: FunctionExecutorStatesContainer,
        config_path: Optional[str],
        downloader: Downloader,
        task_reporter: TaskReporter,
        channel_manager: ChannelManager,
        state_reporter: ExecutorStateReporter,
        logger: Any,
    ):
        self._executor_id: str = executor_id
        self._function_executor_server_factory: FunctionExecutorServerFactory = (
            function_executor_server_factory
        )
        self._base_url: str = base_url
        self._config_path: Optional[str] = config_path
        self._downloader: Downloader = downloader
        self._task_reporter: TaskReporter = task_reporter
        self._channel_manager: ChannelManager = channel_manager
        self._state_reporter: ExecutorStateReporter = state_reporter
        self._reconciliation_loop_task: Optional[asyncio.Task] = None
        self._logger: Any = logger.bind(module=__name__)

        # Mutable state. Doesn't need lock because we access from async tasks running in the same thread.
        self._is_shutdown: bool = False
        self._function_executor_states: FunctionExecutorStatesContainer = (
            function_executor_states
        )
        self._function_executor_controllers: Dict[str, FunctionExecutorController] = {}
        self._task_controllers: Dict[str, TaskController] = {}
        self._last_server_clock: Optional[int] = None

        self._last_desired_state_lock = asyncio.Lock()
        self._last_desired_state_change_notifier: asyncio.Condition = asyncio.Condition(
            lock=self._last_desired_state_lock
        )
        self._last_desired_state: Optional[DesiredExecutorState] = None

    async def run(self):
        """Runs the state reconciler.

        Never raises any exceptions.
        """
        self._reconciliation_loop_task = asyncio.create_task(
            self._reconciliation_loop(),
            name="state reconciler reconciliation loop",
        )

        # TODO: Move this into a new async task and cancel it in shutdown().
        while not self._is_shutdown:
            stub = ExecutorAPIStub(await self._channel_manager.get_channel())
            while not self._is_shutdown:
                try:
                    # Report state once before starting the stream so Server
                    # doesn't use stale state it knew about this Executor in the past.
                    await self._state_reporter.report_state(stub)

                    desired_states_stream: AsyncGenerator[
                        DesiredExecutorState, None
                    ] = stub.get_desired_executor_states(
                        GetDesiredExecutorStatesRequest(executor_id=self._executor_id)
                    )
                    await self._process_desired_states_stream(desired_states_stream)
                except Exception as e:
                    self._logger.error(
                        f"Failed processing desired states stream, reconnecting in {_RECONCILE_STREAM_BACKOFF_INTERVAL_SEC} sec.",
                        exc_info=e,
                    )
                    await asyncio.sleep(_RECONCILE_STREAM_BACKOFF_INTERVAL_SEC)
                    break

    async def _process_desired_states_stream(
        self, desired_states: AsyncGenerator[DesiredExecutorState, None]
    ):
        async for new_state in desired_states:
            if self._is_shutdown:
                return

            new_state: DesiredExecutorState
            validator: MessageValidator = MessageValidator(new_state)
            try:
                validator.required_field("clock")
            except ValueError as e:
                self._logger.error(
                    "Received invalid DesiredExecutorState from Server. Ignoring.",
                    exc_info=e,
                )
                continue

            if self._last_server_clock is not None:
                if self._last_server_clock >= new_state.clock:
                    continue  # Duplicate or outdated message state sent by Server.

            self._last_server_clock = new_state.clock
            # Always read the latest desired state value from the stream so
            # we're never acting on stale desired states.
            async with self._last_desired_state_lock:
                self._last_desired_state = new_state
                self._last_desired_state_change_notifier.notify_all()

    async def shutdown(self):
        """Shuts down the state reconciler.

        Never raises any exceptions.
        """
        self._is_shutdown = True
        if self._reconciliation_loop_task is not None:
            self._reconciliation_loop_task.cancel()
            self._logger.info("Reconciliation loop shutdown.")

        for controller in self._task_controllers.values():
            await controller.destroy()
        # FEs are destroyed in executor.py right now.
        # TODO: Once HTTP loop is removed add all FE state and controllers
        # shutdown logic here. This should allow us to get rid of hacky
        # "cancel all tasks loop" in executor.py shutdown and make the shutdown
        # much more controllable and clean. E.g. we would be able to remove logs
        # suppression from shutdown logic. Also need to shutdown self._function_executor_controllers.

    async def _reconciliation_loop(self):
        last_reconciled_state: Optional[DesiredExecutorState] = None
        while not self._is_shutdown:
            async with self._last_desired_state_lock:
                # Comparing object identities (references) is enough here to not reconcile
                # the same state twice.
                while self._last_desired_state is last_reconciled_state:
                    await self._last_desired_state_change_notifier.wait()
                last_reconciled_state = self._last_desired_state

            with metric_state_reconciliation_latency.time():
                metric_state_reconciliations.inc()
                await self._reconcile_state(last_reconciled_state)
                self._state_reporter.update_last_server_clock(
                    last_reconciled_state.clock
                )

    async def _reconcile_state(self, desired_state: DesiredExecutorState):
        """Reconciles the desired state with the current state.

        Doesn't raise any exceptions. Logs all errors for future investigation becase the gRPC protocol
        doesn't allow us to return errors to the Server if it supplied invalid messages.
        """
        for attempt in range(_RECONCILIATION_RETRIES):
            try:
                # Reconcile FEs first because Tasks depend on them.
                await self._reconcile_function_executors(
                    desired_state.function_executors
                )
                await self._reconcile_tasks(desired_state.task_allocations)
                return
            except Exception as e:
                self._logger.error(
                    "Failed to reconcile desired state. Retrying in 5 secs.",
                    exc_info=e,
                    attempt=attempt,
                    attempts_left=_RECONCILIATION_RETRIES - attempt,
                )
                await asyncio.sleep(5)

        metric_state_reconciliation_errors.inc()
        self._logger.error(
            f"Failed to reconcile desired state after {_RECONCILIATION_RETRIES} attempts.",
        )

    async def _reconcile_function_executors(
        self, function_executor_descriptions: Iterable[FunctionExecutorDescription]
    ):
        valid_fe_descriptions: List[FunctionExecutorDescription] = (
            self._valid_function_executor_descriptions(function_executor_descriptions)
        )
        for fe_description in valid_fe_descriptions:
            await self._reconcile_function_executor(fe_description)

        seen_fe_ids: Set[str] = set(map(lambda fe: fe.id, valid_fe_descriptions))
        fe_ids_to_remove = set(self._function_executor_controllers.keys()) - seen_fe_ids
        for function_executor_id in fe_ids_to_remove:
            # Remove the controller before FE shutdown completes so we won't attempt to do it
            # again on the next reconciliations.
            await self._function_executor_controllers.pop(
                function_executor_id
            ).shutdown()
            # Schedule removal of the FE state after shutdown. This is required for Server
            # to known when exactly FE resources are freed so it can put a replacement FE if needed.
            # Running in a separate asyncio task because this will block until the shutdown is complete.
            asyncio.create_task(
                self._remove_function_executor_after_shutdown(function_executor_id),
                name="Remove Function Executor after shutdown",
            )

    def _valid_function_executor_descriptions(
        self, function_executor_descriptions: Iterable[FunctionExecutorDescription]
    ):
        valid_function_executor_descriptions: List[FunctionExecutorDescription] = []
        for function_executor_description in function_executor_descriptions:
            function_executor_description: FunctionExecutorDescription
            logger = function_executor_logger(
                function_executor_description, self._logger
            )

            try:
                validate_function_executor_description(function_executor_description)
            except ValueError as e:
                logger.error(
                    "Received invalid FunctionExecutorDescription from Server. Dropping it from desired state.",
                    exc_info=e,
                )
                continue

            valid_function_executor_descriptions.append(function_executor_description)

        return valid_function_executor_descriptions

    async def _reconcile_function_executor(
        self, function_executor_description: FunctionExecutorDescription
    ):
        """Reconciles a single Function Executor with the desired state.

        Doesn't block on any long running operations. Doesn't raise any exceptions.
        """
        if function_executor_description.id not in self._function_executor_controllers:
            await self._create_function_executor(function_executor_description)

    async def _create_function_executor(
        self, function_executor_description: FunctionExecutorDescription
    ) -> None:
        """Creates Function Executor for the supplied description.

        Doesn't block on any long running operations. Doesn't raise any exceptions.
        """
        logger = function_executor_logger(function_executor_description, self._logger)
        try:
            # TODO: Store FE description in FE state object once we migrate to gRPC State Reconciler.
            # Then most of these parameters will be removed. Also remove the container and use a simple
            # Dict once FE shutdown logic is moved into reconciler.
            function_executor_state: FunctionExecutorState = (
                await self._function_executor_states.get_or_create_state(
                    id=function_executor_description.id,
                    namespace=function_executor_description.namespace,
                    graph_name=function_executor_description.graph_name,
                    graph_version=function_executor_description.graph_version,
                    function_name=function_executor_description.function_name,
                    image_uri=(
                        function_executor_description.image_uri
                        if function_executor_description.HasField("image_uri")
                        else None
                    ),
                    secret_names=list(function_executor_description.secret_names),
                )
            )
            controller: FunctionExecutorController = FunctionExecutorController(
                executor_id=self._executor_id,
                function_executor_state=function_executor_state,
                function_executor_description=function_executor_description,
                function_executor_server_factory=self._function_executor_server_factory,
                downloader=self._downloader,
                base_url=self._base_url,
                config_path=self._config_path,
                logger=self._logger,
            )
            self._function_executor_controllers[function_executor_description.id] = (
                controller
            )
            # Ask the controller to create the new FE. Task controllers will notice that the FE is eventually
            # IDLE and start running tasks on it. Server currently doesn't explicitly manage the desired FE status.
            await controller.startup()
        except Exception as e:
            logger.error("Failed adding Function Executor", exc_info=e)

    async def _remove_function_executor_after_shutdown(
        self, function_executor_id: str
    ) -> None:
        fe_state: FunctionExecutorState = await self._function_executor_states.get(
            function_executor_id
        )
        async with fe_state.lock:
            await fe_state.wait_status(allowlist=[FunctionExecutorStatus.SHUTDOWN])
        # The whole reconciler could shutdown while we were waiting for the FE to shutdown.
        if not self._is_shutdown:
            await self._function_executor_states.pop(function_executor_id)

    async def _reconcile_tasks(self, task_allocations: Iterable[TaskAllocation]):
        valid_task_allocations: List[TaskAllocation] = self._valid_task_allocations(
            task_allocations
        )
        for task_allocation in valid_task_allocations:
            await self._reconcile_task(task_allocation)

        seen_task_ids: Set[str] = set(
            map(lambda task_allocation: task_allocation.task.id, valid_task_allocations)
        )
        task_ids_to_remove = set(self._task_controllers.keys()) - seen_task_ids
        for task_id in task_ids_to_remove:
            await self._remove_task(task_id)

    async def _reconcile_task(self, task_allocation: TaskAllocation):
        """Reconciles a single TaskAllocation with the desired state.

        Doesn't raise any exceptions.
        """
        if task_allocation.task.id in self._task_controllers:
            # Nothing to do, task allocation already exists and it's immutable.
            return

        logger = self._task_allocation_logger(task_allocation)
        try:
            function_executor_state: FunctionExecutorState = (
                await self._function_executor_states.get(
                    task_allocation.function_executor_id
                )
            )
            self._task_controllers[task_allocation.task.id] = TaskController(
                task=task_allocation.task,
                downloader=self._downloader,
                task_reporter=self._task_reporter,
                function_executor_id=task_allocation.function_executor_id,
                function_executor_state=function_executor_state,
                logger=self._logger,
            )
        except Exception as e:
            logger.error("Failed adding TaskController", exc_info=e)

    async def _remove_task(self, task_id: str) -> None:
        """Schedules removal of an existing task.

        Doesn't block on any long running operations. Doesn't raise any exceptions.
        """
        await self._task_controllers.pop(task_id).destroy()

    def _valid_task_allocations(self, task_allocations: Iterable[TaskAllocation]):
        valid_task_allocations: List[TaskAllocation] = []
        for task_allocation in task_allocations:
            task_allocation: TaskAllocation
            logger = self._task_allocation_logger(task_allocation)

            try:
                validate_task(task_allocation.task)
            except ValueError as e:
                # There's no way to report this error to Server so just log it.
                logger.error(
                    "Received invalid TaskAllocation from Server. Dropping it from desired state.",
                    exc_info=e,
                )
                continue

            validator = MessageValidator(task_allocation)
            try:
                validator.required_field("function_executor_id")
            except ValueError as e:
                # There's no way to report this error to Server so just log it.
                logger.error(
                    "Received invalid TaskAllocation from Server. Dropping it from desired state.",
                    exc_info=e,
                )
                continue

            if (
                task_allocation.function_executor_id
                not in self._function_executor_controllers
            ):
                # Current policy: don't report task outcomes for tasks that didn't run.
                # This is required to simplify the protocol so Server doesn't need to care about task states.
                logger.error(
                    "Received TaskAllocation for a Function Executor that doesn't exist. Dropping it from desired state."
                )
                continue

            valid_task_allocations.append(task_allocation)

        return valid_task_allocations

    def _task_allocation_logger(self, task_allocation: TaskAllocation) -> Any:
        """Returns a logger for the given TaskAllocation.

        Doesn't assume that the supplied TaskAllocation is valid.
        """
        return task_logger(task_allocation.task, self._logger).bind(
            function_executor_id=(
                task_allocation.function_executor_id
                if task_allocation.HasField("function_executor_id")
                else None
            )
        )
