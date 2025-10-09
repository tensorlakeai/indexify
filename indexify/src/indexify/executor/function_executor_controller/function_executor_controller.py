import asyncio
import math
import time
from collections.abc import Coroutine
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List

from tensorlake.function_executor.proto.function_executor_pb2 import (
    ExecutionPlanUpdate as FEExecutionPlanUpdate,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    ExecutionPlanUpdates as FEExecutionPlanUpdates,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    FunctionArg as FEFunctionArg,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    FunctionCall as FEFunctionCall,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    FunctionRef as FEFunctionRef,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    ReduceOp as FEReduceOp,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
)

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.executor.function_executor.function_executor import FunctionExecutor
from indexify.executor.function_executor.health_checker import HealthCheckResult
from indexify.executor.function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerFactory,
)
from indexify.executor.state_reporter import ExecutorStateReporter
from indexify.proto.executor_api_pb2 import (
    Allocation,
    AllocationResult,
    DataPayload,
    DataPayloadEncoding,
    ExecutionPlanUpdate,
    ExecutionPlanUpdates,
    FunctionArg,
    FunctionCall,
    FunctionExecutorDescription,
    FunctionExecutorState,
    FunctionExecutorStatus,
    FunctionExecutorTerminationReason,
    FunctionRef,
    ReduceOp,
)

from .allocation_info import AllocationInfo
from .allocation_input import AllocationInput
from .allocation_output import AllocationOutput
from .completed_allocation_metrics import emit_completed_allocation_metrics
from .create_function_executor import create_function_executor
from .debug_event_loop import (
    debug_print_adding_event,
    debug_print_events,
    debug_print_processing_event,
)
from .events import (
    AllocationExecutionFinished,
    AllocationFinalizationFinished,
    AllocationPreparationFinished,
    BaseEvent,
    EventType,
    FunctionExecutorCreated,
    FunctionExecutorTerminated,
    ScheduleAllocationExecution,
    ShutdownInitiated,
)
from .finalize_allocation import finalize_allocation
from .loggers import allocation_logger, function_executor_logger
from .metrics.function_executor_controller import (
    METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_NOT_STARTED,
    METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_RUNNING,
    METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_STARTING_UP,
    METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_TERMINATED,
    METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_TERMINATING,
    METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_UNKNOWN,
    metric_allocations_fetched,
    metric_control_loop_handle_event_latency,
    metric_function_executors_with_state,
    metric_runnable_allocations,
    metric_runnable_allocations_per_function_name,
    metric_schedule_allocation_latency,
)
from .prepare_allocation import prepare_allocation
from .run_allocation import run_allocation_on_function_executor
from .terminate_function_executor import terminate_function_executor


# Actual FE controller states, they are a bit different from statuses reported to the Server.
# All the valid state transitions are forward only (can skip multiple states in a row).
class _FE_CONTROLLER_STATE(Enum):
    NOT_STARTED = 1
    STARTING_UP = 2
    RUNNING = 3
    TERMINATING = 4
    TERMINATED = 5


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
        self._destroy_lock: asyncio.Lock = asyncio.Lock()
        # Mutable state. No lock needed as it's modified by async tasks running in the same event loop.
        self._fe: FunctionExecutor | None = None
        self._internal_state = _FE_CONTROLLER_STATE.NOT_STARTED
        metric_function_executors_with_state.labels(
            state=_to_fe_state_metric_label(self._internal_state, self._logger)
        ).inc()
        self._reported_state: FunctionExecutorState = FunctionExecutorState(
            description=function_executor_description,
            status=FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_UNKNOWN,
        )
        # Ordered list of events to be processed by the control loop.
        self._events: List[BaseEvent] = []
        # Asyncio event used to notify the control loop that there are new events to process.
        self._event_added: asyncio.Event = asyncio.Event()
        # Control loop asyncio task.
        self._control_loop_aio_task: asyncio.Task | None = None
        # aio tasks spawned by the control loop.
        self._running_aio_tasks: List[asyncio.Task] = []
        # All allocations assigned to FE, Allocation ID -> AllocationInfo.
        self._allocations: Dict[str, AllocationInfo] = {}
        # Allocations prepared for execution on FE.
        self._runnable_allocations: List[AllocationInfo] = []
        # Allocations currently running on the FE.
        self._running_allocations: List[AllocationInfo] = []

    def function_executor_id(self) -> str:
        return self._fe_description.id

    def add_allocation(self, allocation: Allocation) -> None:
        """Adds an allocation to the Function Executor.

        Not blocking. Never raises exceptions.
        """
        logger = allocation_logger(allocation, self._logger)
        if self.has_allocation(allocation.allocation_id):
            logger.warning(
                "attempted to add already added allocation to Function Executor",
            )
            return

        metric_allocations_fetched.inc()
        alloc_info: AllocationInfo = AllocationInfo(
            allocation=allocation,
            allocation_timeout_ms=self._fe_description.allocation_timeout_ms,
            start_time=time.monotonic(),
        )
        self._allocations[allocation.allocation_id] = alloc_info
        next_aio = prepare_allocation(
            alloc_info=alloc_info,
            blob_store=self._blob_store,
            logger=logger,
        )
        self._spawn_aio_for_allocation(
            aio=next_aio,
            alloc_info=alloc_info,
            on_exception=AllocationPreparationFinished(
                alloc_info=alloc_info, is_success=False
            ),
        )

    def has_allocation(self, allocation_id: str) -> bool:
        """Checks if the Function Executor has an allocation with the given ID.

        Not blocking. Never raises exceptions.
        """
        return allocation_id in self._allocations

    def allocation_ids(self) -> List[str]:
        """Returns the list of allocation IDs known to the Function Executor.

        Not blocking. Never raises exceptions.
        """
        return list(self._allocations.keys())

    def remove_allocation(self, allocation_id: str) -> None:
        """Removes the allocation from the Function Executor.

        Cancels the allocation if it's in progress. Just removes the allocation if it was already
        completed. The cancellation is asynchronous and might take a while to complete.
        Until the cancellation is complete, the allocation won't be removed from the Function Executor.
        Not blocking. Never raises exceptions.
        """
        if not self.has_allocation(allocation_id):
            self._logger.warning(
                "attempted to cancel an allocation that is not known to the Function Executor",
                allocation_id=allocation_id,
            )
            return

        alloc_info: AllocationInfo = self._allocations.pop(allocation_id)
        if alloc_info.is_completed:
            return  # Server processed the completed alloc outputs, we can forget it now.

        # Alloc cancellation is required as the alloc is not completed yet.
        logger = allocation_logger(alloc_info.allocation, self._logger)
        alloc_info.is_cancelled = True
        logger.info(
            "cancelling allocation",
        )
        if alloc_info.aio_task is not None:
            alloc_info.aio_task.cancel()

    def startup(self) -> None:
        """Starts up the Function Executor and prepares it to run allocations.

        Not blocking. Never raises exceptions."""
        if self._internal_state != _FE_CONTROLLER_STATE.NOT_STARTED:
            self._logger.warning(
                "function executor state is not NOT_STARTED, ignoring startup call",
                internal_state=self._internal_state.name,
            )
            return

        self._control_loop_aio_task = asyncio.create_task(
            self._control_loop(),
            name="function executor control loop",
        )
        self._update_internal_state(_FE_CONTROLLER_STATE.STARTING_UP)
        self._update_reported_state(
            FunctionExecutorState(
                description=self._fe_description,
                status=FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_PENDING,
            )
        )
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
                fe_termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_INTERNAL_ERROR,
            ),
        )

    async def shutdown(self) -> None:
        """Shutsdown the Function Executor and frees all of its resources.

        No alloc outcomes and outputs are getting reported to Server after this call.
        Doesn't raise any exceptions. Blocks until the shutdown is complete. Idempotent.
        """
        self._add_event(ShutdownInitiated(), source="shutdown")
        try:
            await self._control_loop_aio_task
        except asyncio.CancelledError:
            pass  # Expected exception on shutdown
        except Exception as e:
            self._logger.error(
                "function executor controller control loop task raised unexpected exception",
                exc_info=e,
            )

    def _update_internal_state(self, new_state: _FE_CONTROLLER_STATE) -> None:
        """Updates the internal state of the Function Executor Controller.

        Not blocking. Never raises exceptions."""
        old_state: _FE_CONTROLLER_STATE = self._internal_state
        self._internal_state = new_state

        self._logger.info(
            "function executor internal state changed",
            old_state=old_state.name,
            new_state=new_state.name,
        )

        metric_function_executors_with_state.labels(
            state=_to_fe_state_metric_label(old_state, self._logger)
        ).dec()
        metric_function_executors_with_state.labels(
            state=_to_fe_state_metric_label(new_state, self._logger)
        ).inc()

    def _update_reported_state(
        self,
        new_state: FunctionExecutorState,
    ) -> None:
        """Sets new Function Executor state and reports it to the Server.

        Not blocking. Never raises exceptions."""
        old_state: FunctionExecutorState = self._reported_state
        self._reported_state = new_state

        self._logger.info(
            "function executor grpc status changed",
            old_status=FunctionExecutorStatus.Name(old_state.status),
            new_status=FunctionExecutorStatus.Name(new_state.status),
            termination_reason=_termination_reason_to_short_name(
                new_state.termination_reason
            ),
        )

        self._state_reporter.update_function_executor_state(new_state)
        # Report the status change to the Server asap to reduce latency in the system.
        self._state_reporter.schedule_state_report()

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
                        return await self._shutdown(event)

                    with metric_control_loop_handle_event_latency.time():
                        self._handle_event(event)
                except BaseException as e:
                    # None of the event handlers should raise exceptions, but still catch all exceptions to ensure
                    # that the control loop doesn't crash if an unexpected exception happen.
                    self._logger.error(
                        "unexpected exception in function executor controller control loop",
                        exc_info=e,
                        event_type=event.event_type.name,
                    )
                    if event.event_type == EventType.SHUTDOWN_INITIATED:
                        return  # Unexpected exception during shutdown, should return anyway.

    def _handle_event(self, event: BaseEvent) -> None:
        """Handles the event.

        Doesn't raise any exceptions. Doesn't block.
        """
        if event.event_type == EventType.FUNCTION_EXECUTOR_CREATED:
            return self._handle_event_function_executor_created(event)
        elif event.event_type == EventType.FUNCTION_EXECUTOR_TERMINATED:
            return self._handle_event_function_executor_terminated(event)
        elif event.event_type == EventType.ALLOCATION_PREPARATION_FINISHED:
            return self._handle_event_allocation_preparation_finished(event)
        elif event.event_type == EventType.SCHEDULE_ALLOCATION_EXECUTION:
            return self._handle_event_schedule_allocation_execution(event)
        elif event.event_type == EventType.ALLOCATION_EXECUTION_FINISHED:
            return self._handle_event_allocation_execution_finished(event)
        elif event.event_type == EventType.ALLOCATION_FINALIZATION_FINISHED:
            return self._handle_event_allocation_finalization_finished(event)

        self._logger.warning(
            "unexpected event type received", event_type=event.event_type.name
        )

    def _add_event(self, event: BaseEvent, source: str) -> None:
        """Adds an event to the list of events to be processed by the control loop.

        Doesn't raise any exceptions. Doesn't block."""
        debug_print_adding_event(event=event, source=source, logger=self._logger)
        self._events.append(event)
        self._event_added.set()

    def _spawn_aio_for_allocation(
        self,
        aio: Coroutine[Any, Any, BaseEvent],
        alloc_info: AllocationInfo,
        on_exception: BaseEvent,
    ) -> None:
        self._spawn_aio(
            aio=aio,
            alloc_info=alloc_info,
            on_exception=on_exception,
            logger=allocation_logger(alloc_info.allocation, self._logger),
        )

    def _spawn_aio_for_fe(
        self, aio: Coroutine[Any, Any, BaseEvent], on_exception: BaseEvent
    ) -> None:
        self._spawn_aio(
            aio=aio,
            alloc_info=None,
            on_exception=on_exception,
            logger=self._logger,
        )

    def _spawn_aio(
        self,
        aio: Coroutine[Any, Any, BaseEvent],
        alloc_info: AllocationInfo | None,
        on_exception: BaseEvent,
        logger: Any,
    ) -> None:
        """Spawns an aio task for the supplied coroutine.

        The coroutine should return an event that will be added to the FE controller events.
        The coroutine should not raise any exceptions including BaseException.
        on_exception event will be added to the FE controller events if the aio task raises an unexpected exception.
        on_exception is required to not silently stall the task processing due to an unexpected exception.
        If alloc_info is not None, the aio task will be associated with the alloc_info while the aio task is running.
        Doesn't raise any exceptions. Doesn't block.
        Use `_spawn_aio_for_task_alloc` and `_spawn_aio_for_fe` instead of directly calling this method.
        """

        aio_task_name: str = str(aio)
        # Wrap the coroutine into aio task to disable warning "coroutine was never awaited" when the task is cancelled.
        aio: asyncio.Task = asyncio.create_task(aio, name=aio_task_name)

        async def coroutine_wrapper() -> None:
            try:
                self._add_event(await aio, source=aio_task_name)
            except asyncio.CancelledError:
                # Workaround for scenario when coroutine_wrapper gets cancelled at `await aio` before aio starts.
                # In this case aio doesn't handle the cancellation and doesn't return the right event.
                # A fix for this is to cancel aio instead of coroutine_wrapper. We'll need to keep
                # references to both coroutine_wrapper and aio, cause event loop uses weak references to
                # tasks. Not doing this for now. Using on_exception is good enough because not started aios don't
                # need to do anything special on cancellation.
                self._add_event(on_exception, source=aio_task_name)
            except BaseException as e:
                logger.error(
                    "unexpected exception in aio task",
                    exc_info=e,
                    aio_task_name=aio_task_name,
                )
                self._add_event(on_exception, source=aio_task_name)
            finally:
                if alloc_info is not None:
                    alloc_info.aio_task = None
                self._running_aio_tasks.remove(asyncio.current_task())

        aio_wrapper_task: asyncio.Task = asyncio.create_task(
            coroutine_wrapper(),
            name=f"function executor controller aio task '{aio_task_name}'",
        )
        self._running_aio_tasks.append(aio_wrapper_task)
        if alloc_info is not None:
            alloc_info.aio_task = aio_wrapper_task

    # Event handlers for the events added to the control loop.
    # All the event handlers are synchronous and never block on any long running operations.

    def _handle_event_function_executor_created(
        self, event: FunctionExecutorCreated
    ) -> None:
        """Handles the startup finished event.

        Doesn't raise any exceptions. Doesn't block.
        """
        if event.function_executor is None:
            # Server needs to increment attempts counter for all the allocs that were pending while FE was starting up.
            # This prevents infinite retries if FEs consistently fail to start up.
            # The allocations we marked here also need to not used FE terminated failure reason in their outputs
            # because FE terminated means that the allocation wasn't the cause of the FE termination.
            allocation_ids_caused_termination: List[str] = []
            for alloc_info in self._allocations.values():
                alloc_logger = allocation_logger(alloc_info.allocation, self._logger)
                alloc_logger.info(
                    "marking allocation failed on function executor startup failure"
                )
                allocation_ids_caused_termination.append(
                    alloc_info.allocation.allocation_id
                )
                alloc_info.output = AllocationOutput.function_executor_startup_failed(
                    allocation=alloc_info.allocation,
                    fe_termination_reason=event.fe_termination_reason,
                    logger=alloc_logger,
                )
            self._start_termination(
                fe_termination_reason=event.fe_termination_reason,
                allocation_ids_caused_termination=allocation_ids_caused_termination,
            )
            return

        self._fe = event.function_executor
        self._update_internal_state(_FE_CONTROLLER_STATE.RUNNING)
        self._update_reported_state(
            FunctionExecutorState(
                description=self._fe_description,
                status=FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_RUNNING,
            )
        )
        # Health checker starts after FE creation and gets automatically stopped on FE destroy.
        self._fe.health_checker().start(self._health_check_failed_callback)
        self._add_event(
            ScheduleAllocationExecution(),
            source="_handle_event_function_executor_created",
        )

    def _handle_event_function_executor_terminated(
        self, event: FunctionExecutorTerminated
    ) -> None:
        """Handles the Function Executor terminated event.

        Doesn't raise any exceptions. Doesn't block.
        """
        if not event.is_success:
            self._logger.error(
                "Function Executor termination failed unexpectedly, this should never happen",
            )

        self._fe = None
        # Set reported status only after the FE got destroyed because Server assumes that all FE resources are freed when the status changes.
        self._update_reported_state(
            FunctionExecutorState(
                description=self._fe_description,
                status=FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_TERMINATED,
                termination_reason=event.fe_termination_reason,
                allocation_ids_caused_termination=event.allocation_ids_caused_termination,
            )
        )
        self._update_internal_state(_FE_CONTROLLER_STATE.TERMINATED)

        # Invoke the scheduler so it can fail runnable allocs with FE Terminated error.
        self._add_event(
            ScheduleAllocationExecution(),
            source="_handle_event_function_executor_destroyed",
        )

    async def _health_check_failed_callback(self, result: HealthCheckResult):
        self._logger.error(
            "Function Executor health check failed, terminating Function Executor",
            reason=result.reason,
        )

        self._start_termination(
            fe_termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_UNHEALTHY,
            allocation_ids_caused_termination=[
                alloc_info.allocation.allocation_id
                for alloc_info in self._running_allocations
            ],
        )

    def _handle_event_allocation_preparation_finished(
        self, event: AllocationPreparationFinished
    ) -> None:
        """Handles the allocation preparation finished event.

        Doesn't raise any exceptions. Doesn't block.
        """
        alloc_info: AllocationInfo = event.alloc_info

        if alloc_info.is_cancelled:
            alloc_info.output = AllocationOutput.allocation_cancelled(
                allocation=alloc_info.allocation,
                # Task alloc was never executed
                execution_start_time=None,
                execution_end_time=None,
            )
            self._start_allocation_finalization(alloc_info)
            return

        if not event.is_success:
            # Failed to prepare the alloc inputs.
            alloc_info.output = AllocationOutput.internal_error(
                allocation=alloc_info.allocation,
                # Alloc was never executed
                execution_start_time=None,
                execution_end_time=None,
            )
            self._start_allocation_finalization(alloc_info)
            return

        alloc_info.prepared_time = time.monotonic()
        metric_runnable_allocations.inc()
        metric_runnable_allocations_per_function_name.labels(
            alloc_info.allocation.function.function_name
        ).inc()
        self._runnable_allocations.append(alloc_info)
        self._add_event(
            ScheduleAllocationExecution(),
            source="_handle_event_allocation_preparation_finished",
        )

    def _handle_event_schedule_allocation_execution(
        self, event: ScheduleAllocationExecution
    ) -> None:
        if len(self._runnable_allocations) == 0:
            return

        if self._internal_state not in [
            _FE_CONTROLLER_STATE.RUNNING,
            _FE_CONTROLLER_STATE.TERMINATING,
            _FE_CONTROLLER_STATE.TERMINATED,
        ]:
            return  # Can't progress runnable allocs in the current state.

        if (
            self._internal_state == _FE_CONTROLLER_STATE.RUNNING
            and len(self._running_allocations) == self._fe_description.max_concurrency
        ):
            return

        # Take the next alloc from head to get FIFO order and improve fairness.
        alloc_info: AllocationInfo = self._pop_runnable_allocation()
        # Re-invoke the scheduler later to process the next runnable task if this one can't run on FE.
        self._add_event(
            ScheduleAllocationExecution(),
            source="_handle_event_schedule_allocation_execution",
        )

        if alloc_info.is_cancelled:
            alloc_info.output = AllocationOutput.allocation_cancelled(
                allocation=alloc_info.allocation,
                # Alloc was never executed
                execution_start_time=None,
                execution_end_time=None,
            )
            self._start_allocation_finalization(alloc_info)
        elif self._internal_state in [
            _FE_CONTROLLER_STATE.TERMINATING,
            _FE_CONTROLLER_STATE.TERMINATED,
        ]:
            # The output could be set already by FE startup failure handler.
            if alloc_info.output is None:
                alloc_info.output = AllocationOutput.function_executor_terminated(
                    alloc_info.allocation
                )
            self._start_allocation_finalization(alloc_info)
        elif self._internal_state == _FE_CONTROLLER_STATE.RUNNING:
            self._running_allocations.append(alloc_info)
            next_aio = run_allocation_on_function_executor(
                alloc_info=alloc_info,
                function_executor=self._fe,
                logger=allocation_logger(alloc_info.allocation, self._logger),
            )
            self._spawn_aio_for_allocation(
                aio=next_aio,
                alloc_info=alloc_info,
                on_exception=AllocationExecutionFinished(
                    alloc_info=alloc_info,
                    function_executor_termination_reason=FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_INTERNAL_ERROR,
                ),
            )
        else:
            allocation_logger(alloc_info.allocation, self._logger).error(
                "failed to schedule allocation execution, this should never happen"
            )

    def _pop_runnable_allocation(self) -> AllocationInfo:
        alloc_info: AllocationInfo = self._runnable_allocations.pop(0)
        metric_schedule_allocation_latency.observe(
            time.monotonic() - alloc_info.prepared_time
        )
        metric_runnable_allocations.dec()
        metric_runnable_allocations_per_function_name.labels(
            alloc_info.allocation.function.function_name
        ).dec()
        return alloc_info

    def _handle_event_allocation_execution_finished(
        self, event: AllocationExecutionFinished
    ) -> None:
        """Handles the allocation execution finished event.

        Doesn't raise any exceptions. Doesn't block.
        """
        alloc_info: AllocationInfo = event.alloc_info
        self._running_allocations.remove(alloc_info)

        if event.function_executor_termination_reason is None:
            self._add_event(
                ScheduleAllocationExecution(),
                source="_handle_event_allocation_execution_finished",
            )
        else:
            self._start_termination(
                fe_termination_reason=event.function_executor_termination_reason,
                allocation_ids_caused_termination=[alloc_info.allocation.allocation_id],
            )

        if alloc_info.output is None:
            # `run_allocation_on_function_executor` guarantees that the output is set in
            # all cases including allocation cancellations. If this didn't happen then some
            # internal error occurred in our code.
            alloc_info.output = AllocationOutput.internal_error(
                allocation=alloc_info.allocation,
                execution_start_time=None,
                execution_end_time=None,
            )

        self._start_allocation_finalization(alloc_info)

    def _start_allocation_finalization(self, alloc_info: AllocationInfo) -> None:
        """Starts finalization for the given allocation.

        Doesn't raise any exceptions. Doesn't block.
        alloc_info.output should not be None.
        """
        next_aio = finalize_allocation(
            alloc_info=alloc_info,
            blob_store=self._blob_store,
            logger=allocation_logger(alloc_info.allocation, self._logger),
        )
        self._spawn_aio_for_allocation(
            aio=next_aio,
            alloc_info=alloc_info,
            on_exception=AllocationFinalizationFinished(
                alloc_info=alloc_info, is_success=False
            ),
        )

    def _handle_event_allocation_finalization_finished(
        self, event: AllocationFinalizationFinished
    ) -> None:
        """Handles the allocation finalization finished event.

        Doesn't raise any exceptions. Doesn't block.
        """
        alloc_info: AllocationInfo = event.alloc_info
        if not event.is_success:
            original_alloc_output: AllocationOutput = (
                alloc_info.output
            )  # Never None here
            alloc_info.output = AllocationOutput.internal_error(
                allocation=alloc_info.allocation,
                execution_start_time=original_alloc_output.execution_start_time,
                execution_end_time=original_alloc_output.execution_end_time,
            )

        logger: Any = allocation_logger(alloc_info.allocation, self._logger)
        # Ignore alloc cancellation as it's technically finished at this point.
        alloc_info.is_completed = True
        emit_completed_allocation_metrics(
            alloc_info=alloc_info,
            logger=logger,
        )
        # Reconciler will call .remove_allocation() once Server signals that it processed this update.
        self._state_reporter.add_completed_allocation_result(
            _to_alloc_result_proto(alloc_info, logger)
        )
        self._state_reporter.schedule_state_report()

    def _start_termination(
        self,
        fe_termination_reason: FunctionExecutorTerminationReason,
        allocation_ids_caused_termination: List[str],
    ) -> None:
        """Starts termination of the Function Executor if it's not started yet.

        Doesn't raise any exceptions. Doesn't block.
        """
        if self._internal_state in [
            _FE_CONTROLLER_STATE.TERMINATING,
            _FE_CONTROLLER_STATE.TERMINATED,
        ]:
            # _start_termination() can be called multiple times, e.g. by each failed alloc
            # when the FE is unhealthy. Dedup the calls to keep state machine consistent.
            return

        self._update_internal_state(_FE_CONTROLLER_STATE.TERMINATING)
        next_aio = terminate_function_executor(
            function_executor=self._fe,
            lock=self._destroy_lock,
            fe_termination_reason=fe_termination_reason,
            allocation_ids_caused_termination=allocation_ids_caused_termination,
            logger=self._logger,
        )
        self._spawn_aio_for_fe(
            aio=next_aio,
            on_exception=FunctionExecutorTerminated(
                is_success=False,
                fe_termination_reason=fe_termination_reason,
                allocation_ids_caused_termination=allocation_ids_caused_termination,
            ),
        )

    async def _shutdown(self, event: ShutdownInitiated) -> None:
        """Shuts down the Function Executor and frees all its resources.

        The control loop must be blocked while this method is running.
        The control loop must exit immediately after this method returns.
        Doesn't raise any exceptions.

        Server needs to wait until all the allocations its interested in got their outcomes reported
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

        # Makes sure we don't run fe destroy concurrently with an event loop task.
        # FE destroy uses asyncio.to_thread() calls so it doesn't get cancelled with all the tasks above.
        async with self._destroy_lock:
            if self._fe is not None:
                self._logger.info(
                    "destroying function executor",
                )
                await self._fe.destroy()

        # Cleanup the metric from this FE.
        metric_function_executors_with_state.labels(
            state=_to_fe_state_metric_label(self._internal_state, self._logger)
        ).dec()

        self._state_reporter.remove_function_executor_state(self.function_executor_id())
        self._state_reporter.schedule_state_report()

        self._logger.info("function executor controller shutdown finished")
        debug_print_events(events=self._events, logger=self._logger)


def _to_fe_state_metric_label(state: _FE_CONTROLLER_STATE, logger: Any) -> str:
    if state == _FE_CONTROLLER_STATE.NOT_STARTED:
        return METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_NOT_STARTED
    elif state == _FE_CONTROLLER_STATE.STARTING_UP:
        return METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_STARTING_UP
    elif state == _FE_CONTROLLER_STATE.RUNNING:
        return METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_RUNNING
    elif state == _FE_CONTROLLER_STATE.TERMINATING:
        return METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_TERMINATING
    elif state == _FE_CONTROLLER_STATE.TERMINATED:
        return METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_TERMINATED
    else:
        logger.error(
            "unexpected Function Executor internal state",
            state=state.name,
        )
        return METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_UNKNOWN


_termination_reason_to_short_name_map = {
    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_UNKNOWN: "UNKNOWN",
    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_INTERNAL_ERROR: "STARTUP_FAILED_INTERNAL_ERROR",
    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR: "STARTUP_FAILED_FUNCTION_ERROR",
    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT: "STARTUP_FAILED_FUNCTION_TIMEOUT",
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


def _to_alloc_result_proto(alloc_info: AllocationInfo, logger: Any) -> AllocationResult:
    allocation: Allocation = alloc_info.allocation
    # Might be None if the alloc wasn't prepared successfully.
    input: AllocationInput | None = alloc_info.input
    # Never None here as we're completing the alloc here.
    output: AllocationOutput | None = alloc_info.output

    execution_duration_ms: int | None = None
    if (
        output.execution_start_time is not None
        and output.execution_end_time is not None
    ):
        # <= 0.99 ms functions get billed as 1 ms.
        execution_duration_ms = math.ceil(
            (output.execution_end_time - output.execution_start_time) * 1000
        )

    request_error_output: DataPayload | None = None
    if output.request_error_output is not None:
        # input can't be None if request_error_output is set because the alloc ran already.
        request_error_output = _so_to_data_payload_proto(
            so=output.request_error_output,
            blob_uri=input.request_error_blob_uri,
            logger=logger,
        )

    output_value: DataPayload | None = None
    if output.output_value is not None:
        # input can't be None if output_value is set because the alloc ran already.
        output_value = _so_to_data_payload_proto(
            so=output.output_value,
            blob_uri=input.function_outputs_blob_uri,
            logger=logger,
        )

    output_updates: ExecutionPlanUpdates | None = None
    if output.output_execution_plan_updates is not None:
        output_updates = _to_execution_plan_updates_proto(
            fe_updates=output.output_execution_plan_updates,
            output_blob_uri=input.function_outputs_blob_uri,
            logger=logger,
        )

    return AllocationResult(
        function=FunctionRef(
            namespace=allocation.function.namespace,
            application_name=allocation.function.application_name,
            function_name=allocation.function.function_name,
            application_version=allocation.function.application_version,
        ),
        allocation_id=allocation.allocation_id,
        function_call_id=allocation.function_call_id,
        request_id=allocation.request_id,
        outcome_code=output.outcome_code,
        failure_reason=output.failure_reason,
        value=output_value,
        updates=output_updates,
        request_error=request_error_output,
        execution_duration_ms=execution_duration_ms,
    )


def _to_execution_plan_updates_proto(
    fe_updates: FEExecutionPlanUpdates, output_blob_uri: str, logger: Any
) -> ExecutionPlanUpdates:
    # TODO: Validate FEExecutionPlanUpdates object.
    executor_updates: List[ExecutionPlanUpdate] = []
    for fe_update in fe_updates.updates:
        fe_update: FEExecutionPlanUpdate

        if fe_update.HasField("function_call"):
            executor_updates.append(
                ExecutionPlanUpdate(
                    function_call=_to_function_call_proto(
                        fe_function_call=fe_update.function_call,
                        output_blob_uri=output_blob_uri,
                        logger=logger,
                    )
                )
            )
        elif fe_update.HasField("reduce"):
            executor_updates.append(
                ExecutionPlanUpdate(
                    reduce=_to_reduce_op_proto(
                        reduce=fe_update.reduce,
                        output_blob_uri=output_blob_uri,
                        logger=logger,
                    )
                )
            )
        else:
            logger.error(
                "unexpected FEExecutionPlanUpdate with no function_call or reduce set",
            )

    return ExecutionPlanUpdates(
        updates=executor_updates,
        root_function_call_id=fe_updates.root_function_call_id,
    )


def _to_function_call_proto(
    fe_function_call: FEFunctionCall, output_blob_uri: str, logger: Any
) -> FunctionCall:
    args: List[FunctionArg] = []
    for fe_arg in fe_function_call.args:
        fe_arg: FEFunctionArg
        args.append(
            _to_function_arg_proto(
                fe_function_arg=fe_arg,
                output_blob_uri=output_blob_uri,
                logger=logger,
            )
        )

    return FunctionCall(
        id=fe_function_call.id,
        target=_to_function_ref_proto(fe_function_call.target),
        args=args,
        call_metadata=fe_function_call.call_metadata,
    )


def _to_reduce_op_proto(
    reduce: FEReduceOp, output_blob_uri: str, logger: Any
) -> ReduceOp:
    collection: List[FunctionArg] = []
    for fe_arg in reduce.collection:
        fe_arg: FEFunctionArg
        collection.append(
            _to_function_arg_proto(
                fe_function_arg=fe_arg,
                output_blob_uri=output_blob_uri,
                logger=logger,
            )
        )

    return ReduceOp(
        id=reduce.id,
        collection=collection,
        reducer=_to_function_ref_proto(reduce.reducer),
        call_metadata=reduce.call_metadata,
    )


def _to_function_ref_proto(fe_function_ref: FEFunctionRef) -> FunctionRef:
    return FunctionRef(
        namespace=fe_function_ref.namespace,
        application_name=fe_function_ref.application_name,
        function_name=fe_function_ref.function_name,
        application_version=fe_function_ref.application_version,
    )


def _to_function_arg_proto(
    fe_function_arg: FEFunctionArg, output_blob_uri: str, logger: Any
) -> FunctionArg:
    if fe_function_arg.HasField("function_call_id"):
        return FunctionArg(function_call_id=fe_function_arg.function_call_id)
    elif fe_function_arg.HasField("value"):
        return FunctionArg(
            inline_data=_so_to_data_payload_proto(
                so=fe_function_arg.value,
                blob_uri=output_blob_uri,
                logger=logger,
            )
        )
    else:
        logger.error(
            "unexpected FEFunctionArg with no value or function_call_id set",
        )
        return FunctionArg()  # Empty arg as we can't raise here.


def _so_to_data_payload_proto(
    so: SerializedObjectInsideBLOB,
    blob_uri: str,
    logger: Any,
) -> DataPayload:
    """Converts a serialized object inside BLOB to into a DataPayload."""
    # TODO: Validate SerializedObjectInsideBLOB.
    return DataPayload(
        uri=blob_uri,
        encoding=_so_to_data_payload_encoding(so.manifest.encoding, logger),
        encoding_version=so.manifest.encoding_version,
        content_type=(
            so.manifest.content_type if so.manifest.HasField("content_type") else None
        ),
        metadata_size=so.manifest.metadata_size,
        offset=so.offset,
        size=so.manifest.size,
        sha256_hash=so.manifest.sha256_hash,
        source_function_call_id=so.manifest.source_function_call_id,
        # id is not used
    )


def _so_to_data_payload_encoding(
    encoding: SerializedObjectEncoding, logger: Any
) -> DataPayloadEncoding:
    if encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE:
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_BINARY_PICKLE
    elif encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON:
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_JSON
    elif encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT:
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_TEXT
    elif encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_RAW:
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_RAW
    else:
        logger.error(
            "unexpected encoding for SerializedObject",
            encoding=SerializedObjectEncoding.Name(encoding),
        )
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UNKNOWN
