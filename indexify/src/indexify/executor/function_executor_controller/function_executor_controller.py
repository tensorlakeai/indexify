import asyncio
import time
from collections.abc import Coroutine
from pathlib import Path
from typing import Any, Dict, List, Optional

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.executor.function_executor.function_executor import FunctionExecutor
from indexify.executor.function_executor.health_checker import HealthCheckResult
from indexify.executor.function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerFactory,
)
from indexify.executor.state_reporter import ExecutorStateReporter
from indexify.proto.executor_api_pb2 import (
    FunctionExecutorDescription,
    FunctionExecutorState,
    FunctionExecutorStatus,
    FunctionExecutorTerminationReason,
    FunctionExecutorUpdate,
    TaskAllocation,
    TaskResult,
)

from .completed_task_metrics import emit_completed_task_metrics
from .create_function_executor import create_function_executor
from .debug_event_loop import (
    debug_print_adding_event,
    debug_print_events,
    debug_print_processing_event,
)
from .destroy_function_executor import destroy_function_executor
from .events import (
    BaseEvent,
    EventType,
    FunctionExecutorCreated,
    FunctionExecutorDestroyed,
    ScheduleTaskExecution,
    ShutdownInitiated,
    TaskExecutionFinished,
    TaskOutputUploadFinished,
    TaskPreparationFinished,
)
from .function_executor_startup_output import FunctionExecutorStartupOutput
from .loggers import function_executor_logger, task_allocation_logger
from .metrics.function_executor_controller import (
    METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_PENDING,
    METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_RUNNING,
    METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_TERMINATED,
    METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_UNKNOWN,
    metric_control_loop_handle_event_latency,
    metric_function_executors_with_status,
    metric_runnable_tasks,
    metric_runnable_tasks_per_function_name,
    metric_schedule_task_latency,
    metric_tasks_fetched,
)
from .prepare_task import prepare_task
from .run_task import run_task_on_function_executor
from .task_info import TaskInfo
from .task_output import TaskOutput
from .upload_task_output import upload_task_output


class FunctionExecutorController:
    def __init__(
        self,
        executor_id: str,
        function_executor_description: FunctionExecutorDescription,
        function_executor_server_factory: FunctionExecutorServerFactory,
        state_reporter: ExecutorStateReporter,
        blob_store: BLOBStore,
        base_url: str,
        config_path: str,
        cache_path: Path,
        logger: Any,
    ):
        """Initializes the FunctionExecutorController.

        The supplied FunctionExecutorDescription must be already validated by the caller
        using validate_function_executor_description().
        """
        self._executor_id: str = executor_id
        self._fe_description: FunctionExecutorDescription = (
            function_executor_description
        )
        self._fe_server_factory: FunctionExecutorServerFactory = (
            function_executor_server_factory
        )
        self._state_reporter: ExecutorStateReporter = state_reporter
        self._blob_store: BLOBStore = blob_store
        self._base_url: str = base_url
        self._config_path: str = config_path
        self._cache_path: Path = cache_path
        self._logger: Any = function_executor_logger(
            function_executor_description, logger.bind(module=__name__)
        )
        # Mutable state. No lock needed as it's modified by async tasks running in
        # the same event loop.
        self._fe: Optional[FunctionExecutor] = None
        self._fe_termination_reason: FunctionExecutorTerminationReason = (
            None  # Optional
        )
        # FE Status reported to Server.
        self._status: FunctionExecutorStatus = (
            FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_UNKNOWN
        )
        metric_function_executors_with_status.labels(
            status=_to_fe_status_metric_label(self._status, self._logger)
        ).inc()
        # Ordered list of events to be processed by the control loop.
        self._events: List[BaseEvent] = []
        # Asyncio event used to notify the control loop that there are new events to process.
        self._event_added: asyncio.Event = asyncio.Event()
        # Control loop asyncio task.
        self._control_loop_aio_task: Optional[asyncio.Task] = None
        # aio tasks spawned by the control loop.
        self._running_aio_tasks: List[asyncio.Task] = []
        # Info for all known tasks, Task ID -> TaskInfo.
        self._tasks: Dict[str, TaskInfo] = {}
        # Tracking of task execution on Function Executor.
        self._runnable_tasks: List[TaskInfo] = []
        self._running_task: Optional[TaskInfo] = None

    def function_executor_id(self) -> str:
        return self._fe_description.id

    def status(self) -> FunctionExecutorStatus:
        """Returns the current status of the Function Executor.

        Not blocking.
        """
        return self._status

    def add_task_allocation(self, task_allocation: TaskAllocation) -> None:
        """Adds a task to the Function Executor.

        Not blocking. Never raises exceptions.
        """
        logger = task_allocation_logger(task_allocation, self._logger)
        if self.has_task(task_allocation.task.id):
            logger.warning(
                "attempted to add already added task to Function Executor",
            )
            return

        metric_tasks_fetched.inc()
        task_info: TaskInfo = TaskInfo(
            allocation=task_allocation, start_time=time.monotonic()
        )
        self._tasks[task_allocation.task.id] = task_info
        next_aio = prepare_task(
            task_info=task_info,
            blob_store=self._blob_store,
            logger=logger,
        )
        self._spawn_aio_for_task(
            aio=next_aio,
            task_info=task_info,
            on_exception=TaskPreparationFinished(task_info=task_info, is_success=False),
        )

    def has_task(self, task_id: str) -> bool:
        """Checks if the Function Executor has a task with the given ID.

        Not blocking. Never raises exceptions.
        """
        return task_id in self._tasks

    def task_ids(self) -> List[str]:
        """Returns the list of task IDs known to the Function Executor.

        Not blocking. Never raises exceptions.
        """
        return list(self._tasks.keys())

    def remove_task(self, task_id: str) -> None:
        """Removes the task from the Function Executor.

        Cancels the task if it's in progress. Just removes the task if it was already completed.
        The cancellation is asynchronous and might take a while to complete.
        Until the cancellation is complete, the task won't be removed from the Function Executor.
        Not blocking. Never raises exceptions.
        """
        if not self.has_task(task_id):
            self._logger.warning(
                "attempted to cancel a task that is not known to the Function Executor",
                task_id=task_id,
            )
            return

        task_info: TaskInfo = self._tasks.pop(task_id)
        if task_info.is_completed:
            return  # Server processed the completed task outputs, we can forget it now.

        # Task cancellation is required as the task is not completed yet.
        logger = task_allocation_logger(task_info.allocation, self._logger)
        task_info.is_cancelled = True
        logger.info(
            "cancelling task",
            allocation_id=task_info.allocation_id,
        )
        if task_info.aio_task is not None:
            task_info.aio_task.cancel()

    def startup(self) -> None:
        """Starts up the Function Executor and prepares it to run tasks.

        Not blocking. Never raises exceptions."""
        if self._control_loop_aio_task is not None:
            self._logger.warning(
                "ignoring startup call as the Function Executor is already started"
            )
            return

        self._control_loop_aio_task = asyncio.create_task(
            self._control_loop(),
            name="function executor control loop",
        )
        self._set_status(FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_PENDING)
        next_aio = create_function_executor(
            function_executor_description=self._fe_description,
            function_executor_server_factory=self._fe_server_factory,
            blob_store=self._blob_store,
            executor_id=self._executor_id,
            base_url=self._base_url,
            config_path=self._config_path,
            cache_path=self._cache_path,
            logger=self._logger,
        )
        self._spawn_aio_for_fe(
            aio=next_aio,
            on_exception=FunctionExecutorCreated(
                function_executor=None,
                output=FunctionExecutorStartupOutput(
                    function_executor_description=self._fe_description,
                    termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_INTERNAL_ERROR,
                ),
            ),
        )

    async def shutdown(
        self, termination_reason: FunctionExecutorTerminationReason
    ) -> None:
        """Shutsdown the Function Executor and frees all of its resources.

        All the tasks are reported as failed with FE Terminated failure code.
        Doesn't raise any exceptions. Blocks until the shutdown is complete.
        """
        self._add_event(
            ShutdownInitiated(termination_reason=termination_reason), source="shutdown"
        )
        try:
            await self._control_loop_aio_task
        except asyncio.CancelledError:
            pass  # Expected exception on shutdown
        except Exception as e:
            self._logger.error(
                "function executor controller control loop raised unexpected exception",
                exc_info=e,
            )
        self._logger.info("function executor controller shutdown finished")

    def _set_status(
        self,
        status: FunctionExecutorStatus,
    ) -> None:
        """Sets Function Executor status and reports it to the Server.

        Not blocking. Never raises exceptions."""
        old_status: FunctionExecutorStatus = self._status
        new_status: FunctionExecutorStatus = status
        self._status: FunctionExecutorStatus = new_status

        self._logger.info(
            "function executor status changed",
            old_status=FunctionExecutorStatus.Name(old_status),
            new_status=FunctionExecutorStatus.Name(new_status),
            termination_reason=_termination_reason_to_short_name(
                self._fe_termination_reason
            ),
        )
        metric_function_executors_with_status.labels(
            status=_to_fe_status_metric_label(old_status, self._logger)
        ).dec()
        metric_function_executors_with_status.labels(
            status=_to_fe_status_metric_label(new_status, self._logger)
        ).inc()

        self._state_reporter.update_function_executor_state(self._current_state())
        # Report the status change to the Server asap to reduce latency in the system.
        self._state_reporter.schedule_state_report()

    def _current_state(self) -> FunctionExecutorState:
        """Returns the current state of the Function Executor.

        Not blocking. Never raises exceptions.
        """
        termination_reason: Optional[FunctionExecutorTerminationReason] = None
        if self._fe_termination_reason is not None:
            termination_reason = self._fe_termination_reason

        return FunctionExecutorState(
            description=self._fe_description,
            status=self._status,
            termination_reason=termination_reason,
        )

    async def _control_loop(self) -> None:
        """Runs control loop that coordinates all the work done by the Function Executor.

        Doesn't raise any Exceptions.
        """
        self._logger.info("function executor controller control loop started")

        while True:
            await self._event_added.wait()
            self._event_added.clear()

            while self._events:
                event: BaseEvent = self._events.pop(0)
                debug_print_processing_event(event, self._logger)

                try:
                    if event.event_type == EventType.SHUTDOWN_INITIATED:
                        return await self._shutdown_no_exceptions(event)

                    with metric_control_loop_handle_event_latency.time():
                        self._handle_event(event)
                except BaseException as e:
                    # None of the event handlers should raise exceptions, but still catch all exceptions to ensure
                    # that the control loop doesn't crash if an unexpected exception happen.
                    self._logger.error(
                        "unexpected exception in function executor controller control loop",
                        exc_info=e,
                        fe_event=str(event),
                    )

    def _handle_event(self, event: BaseEvent) -> None:
        """Handles the event.

        Doesn't raise any exceptions. Doesn't block.
        """
        if event.event_type == EventType.FUNCTION_EXECUTOR_CREATED:
            return self._handle_event_function_executor_created(event)
        elif event.event_type == EventType.FUNCTION_EXECUTOR_DESTROYED:
            return self._handle_event_function_executor_destroyed(event)
        elif event.event_type == EventType.TASK_PREPARATION_FINISHED:
            return self._handle_event_task_preparation_finished(event)
        elif event.event_type == EventType.SCHEDULE_TASK_EXECUTION:
            return self._handle_event_schedule_task_execution(event)
        elif event.event_type == EventType.TASK_EXECUTION_FINISHED:
            return self._handle_event_task_execution_finished(event)
        elif event.event_type == EventType.TASK_OUTPUT_UPLOAD_FINISHED:
            return self._handle_event_task_output_upload_finished(event)

        self._logger.warning(
            "unexpected event type received", event_type=event.event_type.name
        )

    def _add_event(self, event: BaseEvent, source: str) -> None:
        """Adds an event to the list of events to be processed by the control loop.

        Doesn't raise any exceptions. Doesn't block."""
        debug_print_adding_event(event=event, source=source, logger=self._logger)
        self._events.append(event)
        self._event_added.set()

    def _spawn_aio_for_task(
        self,
        aio: Coroutine[Any, Any, BaseEvent],
        task_info: TaskInfo,
        on_exception: BaseEvent,
    ) -> None:
        self._spawn_aio(
            aio=aio,
            task_info=task_info,
            on_exception=on_exception,
            logger=task_allocation_logger(task_info.allocation, self._logger),
        )

    def _spawn_aio_for_fe(
        self, aio: Coroutine[Any, Any, BaseEvent], on_exception: BaseEvent
    ) -> None:
        self._spawn_aio(
            aio=aio,
            task_info=None,
            on_exception=on_exception,
            logger=self._logger,
        )

    def _spawn_aio(
        self,
        aio: Coroutine[Any, Any, BaseEvent],
        task_info: Optional[TaskInfo],
        on_exception: BaseEvent,
        logger: Any,
    ) -> None:
        """Spawns an aio task for the supplied coroutine.

        The coroutine should return an event that will be added to the FE controller events.
        The coroutine should not raise any exceptions.
        on_exception event will be added to the FE controller events if the aio task raises an unexpected exception.
        on_exception is required to not silently stall the task processing due to an unexpected exception.
        If task_info is not None, the aio task will be associated with the task_info while the aio task is running.
        Doesn't raise any exceptions. Doesn't block.
        Use `_spawn_aio_for_task` and `_spawn_aio_for_fe` instead of directly calling this method.
        """

        aio_task_name: str = str(aio)
        # Wrap the coroutine into aio task to disable warning "coroutine was never awaited" when the task is cancelled.
        aio: asyncio.Task = asyncio.create_task(aio, name=aio_task_name)

        async def coroutine_wrapper() -> None:
            try:
                self._add_event(await aio, source=aio_task_name)
            except asyncio.CancelledError:
                pass  # Expected exception on aio task cancellation.
            except BaseException as e:
                logger.error(
                    "unexpected exception in aio task",
                    exc_info=e,
                    aio_task_name=aio_task_name,
                )
                self._add_event(on_exception, source=aio_task_name)
            finally:
                if task_info is not None:
                    task_info.aio_task = None
                self._running_aio_tasks.remove(asyncio.current_task())

        aio_wrapper_task: asyncio.Task = asyncio.create_task(
            coroutine_wrapper(),
            name=f"function executor controller aio task '{aio_task_name}'",
        )
        self._running_aio_tasks.append(aio_wrapper_task)
        if task_info is not None:
            task_info.aio_task = aio_wrapper_task

    # Event handlers for the events added to the control loop.
    # All the event handlers are synchronous and never block on any long running operations.

    def _handle_event_function_executor_created(
        self, event: FunctionExecutorCreated
    ) -> None:
        """Handles the startup finished event.

        Doesn't raise any exceptions. Doesn't block.
        """
        self._state_reporter.add_function_executor_update(
            FunctionExecutorUpdate(
                description=self._fe_description,
                startup_stdout=event.output.stdout,
                startup_stderr=event.output.stderr,
            )
        )
        self._state_reporter.schedule_state_report()

        if event.function_executor is None:
            self._destroy_function_executor_before_termination(
                event.output.termination_reason
            )
            return

        self._fe = event.function_executor
        self._set_status(FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_RUNNING)
        # Health checker starts after FE creation and gets automatically stopped on FE destroy.
        self._fe.health_checker().start(self._health_check_failed_callback)
        self._add_event(
            ScheduleTaskExecution(),
            source="_handle_event_function_executor_created",
        )

    def _handle_event_function_executor_destroyed(
        self, event: FunctionExecutorDestroyed
    ) -> None:
        """Handles the Function Executor destroy finished event.

        Doesn't raise any exceptions. Doesn't block.
        """
        if not event.is_success:
            self._logger.error(
                "Function Executor destroy failed unexpectedly, this should never happen",
            )
        # Set the status only after the FE got destroyed because Server assumes that all FE resources are freed when the status changes.
        self._fe_termination_reason = event.termination_reason
        self._set_status(FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_TERMINATED)
        # Invoke the scheduler so it can fail runnable tasks with FE Terminated error.
        self._add_event(
            ScheduleTaskExecution(),
            source="_handle_event_function_executor_destroyed",
        )

    async def _health_check_failed_callback(self, result: HealthCheckResult):
        self._logger.error(
            "Function Executor health check failed, terminating Function Executor",
            reason=result.reason,
        )
        self._destroy_function_executor_before_termination(
            termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_UNHEALTHY
        )

    def _handle_event_task_preparation_finished(
        self, event: TaskPreparationFinished
    ) -> None:
        """Handles the task preparation finished event.

        Doesn't raise any exceptions. Doesn't block.
        """
        task_info: TaskInfo = event.task_info

        if task_info.is_cancelled:
            task_info.output = TaskOutput.task_cancelled(task_info.allocation)
            self._start_task_output_upload(task_info)
            return
        if not event.is_success:
            task_info.output = TaskOutput.internal_error(task_info.allocation)
            self._start_task_output_upload(task_info)
            return

        task_info.prepared_time = time.monotonic()
        metric_runnable_tasks.inc()
        metric_runnable_tasks_per_function_name.labels(
            task_info.allocation.task.function_name
        ).inc()
        self._runnable_tasks.append(task_info)
        self._add_event(
            ScheduleTaskExecution(),
            source="_handle_event_task_preparation_finished",
        )

    def _handle_event_schedule_task_execution(
        self, event: ScheduleTaskExecution
    ) -> None:
        if len(self._runnable_tasks) == 0:
            return

        if self._status not in [
            FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_RUNNING,
            FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_TERMINATED,
        ]:
            return  # Can't progress pending task with the current status.

        if (
            self._status == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_RUNNING
            and self._running_task is not None
        ):
            return

        # Take the next task from head to get FIFO order and improve fairness.
        task_info: TaskInfo = self._pop_runnable_task()
        # Re-invoke the scheduler later to process the next runnable task if this one can't run on FE.
        self._add_event(
            ScheduleTaskExecution(),
            source="_handle_event_schedule_task_execution",
        )

        if task_info.is_cancelled:
            task_info.output = TaskOutput.task_cancelled(task_info.allocation)
            self._start_task_output_upload(task_info)
        elif self._status == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_TERMINATED:
            task_info.output = TaskOutput.function_executor_terminated(
                task_info.allocation
            )
            self._start_task_output_upload(task_info)
        elif self._status == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_RUNNING:
            self._running_task = task_info
            next_aio = run_task_on_function_executor(
                task_info=task_info,
                function_executor=self._fe,
                logger=task_allocation_logger(task_info.allocation, self._logger),
            )
            self._spawn_aio_for_task(
                aio=next_aio,
                task_info=task_info,
                on_exception=TaskExecutionFinished(
                    task_info=task_info,
                    function_executor_termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_INTERNAL_ERROR,
                ),
            )
        else:
            task_allocation_logger(task_info.allocation, self._logger).error(
                "failed to schedule task execution, this should never happen"
            )

    def _pop_runnable_task(self) -> TaskInfo:
        task_info: TaskInfo = self._runnable_tasks.pop(0)
        metric_schedule_task_latency.observe(time.monotonic() - task_info.prepared_time)
        metric_runnable_tasks.dec()
        metric_runnable_tasks_per_function_name.labels(
            task_info.allocation.task.function_name
        ).dec()
        return task_info

    def _handle_event_task_execution_finished(
        self, event: TaskExecutionFinished
    ) -> None:
        """Handles the task execution finished event.

        Doesn't raise any exceptions. Doesn't block.
        """
        self._running_task = None

        if event.function_executor_termination_reason is None:
            self._add_event(
                ScheduleTaskExecution(), source="_handle_event_task_execution_finished"
            )
        else:
            self._destroy_function_executor_before_termination(
                termination_reason=event.function_executor_termination_reason
            )

        # Ignore is_cancelled because cancelling a task still involves uploading its output.
        # We'll just upload a real output instead of "task cancelled" output.
        # Adds TaskOutputUploadFinished event when done.
        self._start_task_output_upload(event.task_info)

    def _start_task_output_upload(self, task_info: TaskInfo) -> None:
        """Starts the task output upload for the given task.

        Doesn't raise any exceptions. Doesn't block.
        """
        next_aio = upload_task_output(
            task_info=task_info,
            blob_store=self._blob_store,
            logger=task_allocation_logger(task_info.allocation, self._logger),
        )
        self._spawn_aio_for_task(
            aio=next_aio,
            task_info=task_info,
            on_exception=TaskOutputUploadFinished(
                task_info=task_info, is_success=False
            ),
        )

    def _handle_event_task_output_upload_finished(
        self, event: TaskOutputUploadFinished
    ) -> None:
        """Handles the task output upload finished event.

        Doesn't raise any exceptions. Doesn't block.
        """
        # Ignore task cancellation because we need to report it to the server anyway.
        task_info: TaskInfo = event.task_info
        if not event.is_success:
            task_info.output = TaskOutput.internal_error(task_info.allocation)

        self._complete_task(event.task_info)

    def _complete_task(self, task_info: TaskInfo) -> None:
        """Marks the task as completed and reports it to the Server.

        Doesn't raise any exceptions. Doesn't block.
        """
        task_info.is_completed = True
        emit_completed_task_metrics(
            task_info=task_info,
            logger=task_allocation_logger(task_info.allocation, self._logger),
        )
        # Reconciler will call .remove_task() once Server signals that it processed this update.
        self._state_reporter.add_completed_task_result(
            _to_task_result_proto(task_info.output)
        )
        self._state_reporter.schedule_state_report()

    def _destroy_function_executor_before_termination(
        self, termination_reason: FunctionExecutorTerminationReason
    ) -> None:
        """Destroys the Function Executor and frees all its resources to prepare for transitioning to the TERMINATED state.

        Doesn't raise any exceptions. Doesn't block.
        """
        next_aio = destroy_function_executor(
            function_executor=self._fe,
            termination_reason=termination_reason,
            logger=self._logger,
        )
        self._fe = None
        self._spawn_aio_for_fe(
            aio=next_aio,
            on_exception=FunctionExecutorDestroyed(
                is_success=False, termination_reason=termination_reason
            ),
        )

    async def _shutdown_no_exceptions(self, event: ShutdownInitiated) -> None:
        try:
            await self._shutdown(event)
        except BaseException as e:
            # This would result in resource leaks.
            self._logger.error(
                "unexpected exception in function executor controller shutdown, this should never happen",
                exc_info=e,
            )

    async def _shutdown(self, event: ShutdownInitiated) -> None:
        """Shuts down the Function Executor and frees all its resources.

        The control loop must be blocked while this method is running.
        The control loop must exit immediately after this method returns.
        Doesn't raise any exceptions.

        Server needs to wait until all the tasks its interested in got their outcomes reported
        before calling the FE shutdown as we don't report anything on FE shutdown.
        """
        self._logger.info("function executor controller shutdown initiated")
        # Control loop is blocked executing this method, no new aio tasks will be spawned concurrently.
        # Create a copy of the running aio tasks because they remove themselves from the list when they finish.
        cancelled_tasks: List[asyncio.Task] = self._running_aio_tasks.copy()
        for cancelled_task in cancelled_tasks:
            cancelled_task.cancel()

        # Await all aio tasks to make sure that nothing is mutating this FE controller state concurrently.
        for cancelled_task in cancelled_tasks:
            try:
                await cancelled_task
            except BaseException:
                # Ignore any errors as we expect them when cancelling tasks.
                # BaseException includes asyncio.CancelledError which is always raised here.
                pass

        if self._status != FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_TERMINATED:
            self._handle_event_function_executor_destroyed(
                await destroy_function_executor(
                    function_executor=self._fe,
                    termination_reason=event.termination_reason,
                    logger=self._logger,
                )
            )
        metric_function_executors_with_status.labels(
            status=_to_fe_status_metric_label(self._status, self._logger)
        ).dec()

        self._state_reporter.remove_function_executor_state(self.function_executor_id())
        self._state_reporter.schedule_state_report()

        self._logger.info("function executor controller control loop finished")
        debug_print_events(events=self._events, logger=self._logger)


def _to_fe_status_metric_label(status: FunctionExecutorStatus, logger: Any) -> str:
    if status == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_UNKNOWN:
        return METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_UNKNOWN
    elif status == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_PENDING:
        return METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_PENDING
    elif status == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_RUNNING:
        return METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_RUNNING
    elif status == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_TERMINATED:
        return METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_TERMINATED
    else:
        logger.error(
            "unexpected Function Executor status",
            status=FunctionExecutorStatus.Name(status),
        )
        return METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_UNKNOWN


_termination_reason_to_short_name_map = {
    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_UNKNOWN: "UNKNOWN",
    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_INTERNAL_ERROR: "STARTUP_FAILED_INTERNAL_ERROR",
    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR: "STARTUP_FAILED_FUNCTION_ERROR",
    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT: "STARTUP_FAILED_FUNCTION_TIMEOUT",
    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_EXECUTOR_SHUTDOWN: "EXECUTOR_SHUTDOWN",
    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_REMOVED_FROM_DESIRED_STATE: "REMOVED_FROM_DESIRED_STATE",
    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_UNHEALTHY: "UNHEALTHY",
    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_INTERNAL_ERROR: "INTERNAL_ERROR",
    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_TIMEOUT: "FUNCTION_TIMEOUT",
    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_CANCELLED: "FUNCTION_CANCELLED",
}


def _termination_reason_to_short_name(value: FunctionExecutorTerminationReason) -> str:
    # The enum value names are really long, shorten them to make the logs more readable.
    if value is None:
        return "None"

    return _termination_reason_to_short_name_map.get(value, "UNEXPECTED")


def _to_task_result_proto(output: TaskOutput) -> TaskResult:
    task_result = TaskResult(
        task_id=output.allocation.task.id,
        allocation_id=output.allocation.allocation_id,
        namespace=output.allocation.task.namespace,
        graph_name=output.allocation.task.graph_name,
        graph_version=output.allocation.task.graph_version,
        function_name=output.allocation.task.function_name,
        graph_invocation_id=output.allocation.task.graph_invocation_id,
        reducer=output.reducer,
        outcome_code=output.outcome_code,
        next_functions=(output.router_output.edges if output.router_output else []),
        function_outputs=output.uploaded_data_payloads,
    )
    if output.failure_reason is not None:
        task_result.failure_reason = output.failure_reason
    if output.uploaded_stdout is not None:
        task_result.stdout.CopyFrom(output.uploaded_stdout)
    if output.uploaded_stderr is not None:
        task_result.stderr.CopyFrom(output.uploaded_stderr)
    if output.router_output is not None:
        task_result.routing.next_functions[:] = output.router_output.edges

    return task_result
