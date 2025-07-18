import asyncio
from pathlib import Path
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
)

from tensorlake.function_executor.proto.message_validator import MessageValidator

from indexify.proto.executor_api_pb2 import (
    DesiredExecutorState,
    FunctionExecutorDescription,
    GetDesiredExecutorStatesRequest,
    TaskAllocation,
)
from indexify.proto.executor_api_pb2_grpc import ExecutorAPIStub

from .blob_store.blob_store import BLOBStore
from .channel_manager import ChannelManager
from .function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerFactory,
)
from .function_executor_controller import (
    FunctionExecutorController,
    function_executor_logger,
    task_allocation_logger,
    validate_function_executor_description,
    validate_task_allocation,
)
from .metrics.state_reconciler import (
    metric_state_reconciliation_errors,
    metric_state_reconciliation_latency,
    metric_state_reconciliations,
)
from .state_reporter import ExecutorStateReporter

_RECONCILE_STREAM_BACKOFF_INTERVAL_SEC = 5
_RECONCILIATION_RETRIES = 3
# If we didn't get a new desired state from the stream within this timeout then the stream might
# not be healthy due to network disruption. In this case we need to recreate the stream to make
# sure that Server really doesn't want to send us a new state.
_DESIRED_EXECUTOR_STATES_TIMEOUT_SEC = 5 * 60  # 5 minutes


class ExecutorStateReconciler:
    def __init__(
        self,
        executor_id: str,
        function_executor_server_factory: FunctionExecutorServerFactory,
        base_url: str,
        config_path: Optional[str],
        cache_path: Path,
        blob_store: BLOBStore,
        channel_manager: ChannelManager,
        state_reporter: ExecutorStateReporter,
        logger: Any,
        server_backoff_interval_sec: int = _RECONCILE_STREAM_BACKOFF_INTERVAL_SEC,
    ):
        self._executor_id: str = executor_id
        self._function_executor_server_factory: FunctionExecutorServerFactory = (
            function_executor_server_factory
        )
        self._base_url: str = base_url
        self._config_path: Optional[str] = config_path
        self._cache_path: Path = cache_path
        self._blob_store: BLOBStore = blob_store
        self._channel_manager: ChannelManager = channel_manager
        self._state_reporter: ExecutorStateReporter = state_reporter
        self._logger: Any = logger.bind(module=__name__)
        self._server_backoff_interval_sec: int = server_backoff_interval_sec

        # Mutable state. Doesn't need lock because we access from async tasks running in the same thread.
        self._desired_states_reader_task: Optional[asyncio.Task] = None
        self._reconciliation_loop_task: Optional[asyncio.Task] = None
        self._function_executor_controllers: Dict[str, FunctionExecutorController] = {}
        self._shutting_down_fe_ids: Set[str] = set()
        self._last_server_clock: Optional[int] = None

        self._last_desired_state_lock = asyncio.Lock()
        self._last_desired_state_change_notifier: asyncio.Condition = asyncio.Condition(
            lock=self._last_desired_state_lock
        )
        self._last_desired_state: Optional[DesiredExecutorState] = None

    def run(self):
        """Runs the state reconciler.

        Never raises any exceptions. Doesn't block.
        """
        if self._reconciliation_loop_task is not None:
            self._logger.error(
                "reconciliation loop task is already running, skipping run call"
            )
            return

        self._reconciliation_loop_task = asyncio.create_task(
            self._reconciliation_loop(),
            name="state reconciler reconciliation loop",
        )
        self._desired_states_reader_task = asyncio.create_task(
            self._desired_states_reader_loop(),
            name="state reconciler desired states stream reader",
        )

    async def shutdown(self):
        """Shuts down the state reconciler.

        Never raises any exceptions.
        """
        if self._reconciliation_loop_task is not None:
            self._reconciliation_loop_task.cancel()
            try:
                await self._reconciliation_loop_task
            except asyncio.CancelledError:
                # Expected cancellation, nothing to do.
                pass
            self._logger.info("reconciliation loop is shutdown")

        if self._desired_states_reader_task is not None:
            self._desired_states_reader_task.cancel()
            try:
                await self._desired_states_reader_task
            except asyncio.CancelledError:
                # Expected cancellation, nothing to do.
                pass
            self._logger.info("desired states stream reader loop is shutdown")

        # Now all the aio tasks exited so nothing will intervene with our actions from this point.
        fe_shutdown_tasks: List[asyncio.Task] = []
        for fe_controller in self._function_executor_controllers.values():
            fe_shutdown_tasks.append(
                asyncio.create_task(
                    fe_controller.shutdown(),
                    name=f"Shutdown Function Executor {fe_controller.function_executor_id()}",
                )
            )

        # Run all the shutdown tasks concurrently and wait for them to complete.
        for task in fe_shutdown_tasks:
            await task

        self._function_executor_controllers.clear()
        self._logger.info("state reconciler is shutdown")

    async def _desired_states_reader_loop(self):
        """Reads the desired states stream from Server and processes it.

        Never raises any exceptions. Get cancelled via aio task cancellation.
        """
        while True:
            desired_states_stream: Optional[AsyncIterable[DesiredExecutorState]] = None
            try:
                stub = ExecutorAPIStub(await self._channel_manager.get_shared_channel())
                # Report state once before starting the stream so Server
                # doesn't use stale state it knew about this Executor in the past.
                await self._state_reporter.report_state_and_wait_for_completion()

                desired_states_stream = stub.get_desired_executor_states(
                    GetDesiredExecutorStatesRequest(executor_id=self._executor_id)
                )
                self._logger.info("created new desired states stream")
                await self._process_desired_states_stream(desired_states_stream)
            except Exception as e:
                self._logger.error(
                    f"error while processing desired states stream",
                    exc_info=e,
                )
            finally:
                # Cleanly signal Server that the stream is closed by client.
                # See https://stackoverflow.com/questions/72207914/how-to-stop-listening-on-a-stream-in-python-grpc-client
                if desired_states_stream is not None:
                    desired_states_stream.cancel()

            self._logger.info(
                f"desired states stream closed, reconnecting in {self._server_backoff_interval_sec} sec"
            )
            await asyncio.sleep(self._server_backoff_interval_sec)

    async def _process_desired_states_stream(
        self, desired_states: AsyncIterable[DesiredExecutorState]
    ):
        desired_states_iter: AsyncIterator[DesiredExecutorState] = aiter(desired_states)
        while True:
            try:
                new_state: DesiredExecutorState = await asyncio.wait_for(
                    anext(desired_states_iter),
                    timeout=_DESIRED_EXECUTOR_STATES_TIMEOUT_SEC,
                )
            except asyncio.TimeoutError:
                self._logger.info(
                    f"No desired state received from Server within {_DESIRED_EXECUTOR_STATES_TIMEOUT_SEC} sec, recreating the stream to ensure it is healthy"
                )
                break  # Timeout reached, stream might be unhealthy, exit the loop to recreate the stream.

            validator: MessageValidator = MessageValidator(new_state)
            try:
                validator.required_field("clock")
            except ValueError as e:
                self._logger.error(
                    "received invalid DesiredExecutorState from Server, ignoring",
                    exc_info=e,
                )
                continue

            # TODO: The clock is only incremented when function executors have actionable changes and not on new allocations.
            #       Therefore the clock cannot currently be used as an idempotency token.
            # if self._last_server_clock is not None:
            #     if self._last_server_clock >= new_state.clock:
            #         self._logger.warning(
            #             "received outdated DesiredExecutorState from Server, ignoring",
            #             current_clock=self._last_server_clock,
            #             ignored_clock=new_state.clock,
            #         )
            #         continue  # Duplicate or outdated message state sent by Server.

            self._last_server_clock = new_state.clock
            # Always read the latest desired state value from the stream so
            # we're never acting on stale desired states.
            async with self._last_desired_state_lock:
                self._last_desired_state = new_state
                self._last_desired_state_change_notifier.notify_all()

    async def _reconciliation_loop(self):
        """Continuously reconciles the desired state with the current state.

        Never raises any exceptions. Get cancelled via aio task cancellation."""
        last_reconciled_state: Optional[DesiredExecutorState] = None
        while True:
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
                self._reconcile_function_executors(desired_state.function_executors)
                self._reconcile_tasks(desired_state.task_allocations)
                return
            except Exception as e:
                self._logger.error(
                    "failed to reconcile desired state, retrying in 5 secs",
                    exc_info=e,
                    attempt=attempt,
                    attempts_left=_RECONCILIATION_RETRIES - attempt,
                )
                await asyncio.sleep(5)

        metric_state_reconciliation_errors.inc()
        self._logger.error(
            f"failed to reconcile desired state after {_RECONCILIATION_RETRIES} attempts",
        )

    def _reconcile_function_executors(
        self, function_executor_descriptions: Iterable[FunctionExecutorDescription]
    ):
        valid_fe_descriptions: List[FunctionExecutorDescription] = (
            self._valid_function_executor_descriptions(function_executor_descriptions)
        )
        for fe_description in valid_fe_descriptions:
            self._reconcile_function_executor(fe_description)

        seen_fe_ids: Set[str] = set(map(lambda fe: fe.id, valid_fe_descriptions))
        fe_ids_to_remove = set(self._function_executor_controllers.keys()) - seen_fe_ids
        for fe_id in fe_ids_to_remove:
            # Server forgot this FE, so its safe to forget about it now too.
            self._remove_function_executor_controller(fe_id)

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
                    "received invalid FunctionExecutorDescription from Server, dropping it from desired state",
                    exc_info=e,
                )
                continue

            valid_function_executor_descriptions.append(function_executor_description)

        return valid_function_executor_descriptions

    def _reconcile_function_executor(
        self, function_executor_description: FunctionExecutorDescription
    ):
        """Reconciles a single Function Executor with the desired state.

        Doesn't block on any long running operations. Doesn't raise any exceptions.
        """

        if function_executor_description.id not in self._function_executor_controllers:
            self._add_function_executor_controller(function_executor_description)

    def _add_function_executor_controller(
        self, function_executor_description: FunctionExecutorDescription
    ) -> None:
        """Creates Function Executor for the supplied description and adds it to internal data structures.

        Doesn't block on any long running operations. Doesn't raise any exceptions.
        """
        logger = function_executor_logger(function_executor_description, self._logger)
        try:
            controller: FunctionExecutorController = FunctionExecutorController(
                executor_id=self._executor_id,
                function_executor_description=function_executor_description,
                function_executor_server_factory=self._function_executor_server_factory,
                state_reporter=self._state_reporter,
                blob_store=self._blob_store,
                base_url=self._base_url,
                config_path=self._config_path,
                cache_path=self._cache_path,
                logger=self._logger,
            )
            self._function_executor_controllers[function_executor_description.id] = (
                controller
            )
            controller.startup()
        except Exception as e:
            logger.error("failed adding Function Executor", exc_info=e)

    def _remove_function_executor_controller(self, function_executor_id: str) -> None:
        # Don't remove the FE controller from self._function_executor_controllers until
        # its shutdown is complete. Otherwise, if Server re-adds the FE to desired state
        # before FE shutdown completes then we'll have two FE controllers for the same
        # FE ID which results in many bugs.
        if function_executor_id in self._shutting_down_fe_ids:
            return

        self._shutting_down_fe_ids.add(function_executor_id)
        asyncio.create_task(
            self._shutdown_function_executor_controller(function_executor_id),
            name=f"Shutdown Function Executor {function_executor_id}",
        )

    async def _shutdown_function_executor_controller(
        self, function_executor_id: str
    ) -> None:
        # We are not cancelling this aio task in self.shutdown(). Because of this the code here should
        # not fail if the FE controller is not found in internal data structures. It can be removed
        # by self.shutdown() at any time while we're running this aio task.
        fe_controller: Optional[FunctionExecutorController] = (
            self._function_executor_controllers.get(function_executor_id)
        )
        if fe_controller is None:
            return

        await fe_controller.shutdown()
        self._function_executor_controllers.pop(function_executor_id, None)
        self._shutting_down_fe_ids.discard(function_executor_id)

    def _reconcile_tasks(self, task_allocations: Iterable[TaskAllocation]):
        valid_task_allocations: List[TaskAllocation] = self._valid_task_allocations(
            task_allocations
        )
        for task_allocation in valid_task_allocations:
            self._reconcile_task(task_allocation)

        # Cancel tasks that are no longer in the desired state.
        # FE ID => [Task ID]
        desired_task_ids_per_fe: Dict[str, List[str]] = {}
        for task_allocation in valid_task_allocations:
            if task_allocation.function_executor_id not in desired_task_ids_per_fe:
                desired_task_ids_per_fe[task_allocation.function_executor_id] = []
            desired_task_ids_per_fe[task_allocation.function_executor_id].append(
                task_allocation.task.id
            )

        for fe_controller in self._function_executor_controllers.values():
            fe_controller: FunctionExecutorController
            if fe_controller.function_executor_id() in desired_task_ids_per_fe:
                desired_fe_task_ids: Set[str] = set(
                    desired_task_ids_per_fe[fe_controller.function_executor_id()]
                )
            else:
                # No tasks desired for this FE, so cancel all its tasks.
                desired_fe_task_ids: Set[str] = set()
            actual_fe_task_ids: Set[str] = set(fe_controller.task_ids())
            task_ids_to_remove: Set[str] = actual_fe_task_ids - desired_fe_task_ids
            for task_id in task_ids_to_remove:
                fe_controller.remove_task(task_id)

    def _reconcile_task(self, task_allocation: TaskAllocation):
        """Reconciles a single TaskAllocation with the desired state.

        Doesn't raise any exceptions.
        """
        function_executor_controller: FunctionExecutorController = (
            self._function_executor_controllers[task_allocation.function_executor_id]
        )
        if function_executor_controller.has_task(task_allocation.task.id):
            # Nothing to do, task already exists and it's immutable.
            return

        function_executor_controller.add_task_allocation(task_allocation)

    def _valid_task_allocations(self, task_allocations: Iterable[TaskAllocation]):
        valid_task_allocations: List[TaskAllocation] = []
        for task_allocation in task_allocations:
            task_allocation: TaskAllocation
            logger = task_allocation_logger(task_allocation, self._logger)

            try:
                validate_task_allocation(task_allocation)
            except ValueError as e:
                # There's no way to report this error to Server so just log it.
                logger.error(
                    "received invalid TaskAllocation from Server, dropping it from desired state",
                    exc_info=e,
                )
                continue

            if (
                task_allocation.function_executor_id
                not in self._function_executor_controllers
            ):
                logger.error(
                    "received TaskAllocation for a Function Executor that doesn't exist, dropping it from desired state"
                )
                continue

            valid_task_allocations.append(task_allocation)

        return valid_task_allocations
