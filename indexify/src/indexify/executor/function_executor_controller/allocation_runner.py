import asyncio
import math
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, Set

import grpc
from tensorlake.function_executor.proto.function_executor_pb2 import BLOB as FEBLOB
from tensorlake.function_executor.proto.function_executor_pb2 import (
    Allocation as FEAllocation,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationFailureReason as FEAllocationFailureReason,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationFunctionCall as FEAllocationFunctionCall,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationFunctionCallCreationResult as FEAllocationFunctionCallCreationResult,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationFunctionCallResult as FEAllocationFunctionCallResult,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationFunctionCallWatcher as FEAllocationFunctionCallWatcher,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationOutcomeCode as FEAllocationOutcomeCode,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationOutputBLOB as FEAllocationOutputBLOB,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationOutputBLOBRequest as FEAllocationOutputBLOBRequest,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationProgress as FEAllocationProgress,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationRequestStateCommitWriteOperationResult as FEAllocationRequestStateCommitWriteOperationResult,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationRequestStateOperation as FEAllocationRequestStateOperation,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationRequestStateOperationResult as FEAllocationRequestStateOperationResult,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationRequestStatePrepareReadOperationResult as FEAllocationRequestStatePrepareReadOperationResult,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationRequestStatePrepareWriteOperationResult as FEAllocationRequestStatePrepareWriteOperationResult,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationResult as FEAllocationResult,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationState as FEAllocationState,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationUpdate as FEAllocationUpdate,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    CreateAllocationRequest as FECreateAllocationRequest,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    DeleteAllocationRequest as FEDeleteAllocationRequest,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    ExecutionPlanUpdates as FEExecutionPlanUpdates,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    WatchAllocationStateRequest as FEWatchAllocationStateRequest,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.message_validator import (
    MessageValidator,
)
from tensorlake.function_executor.proto.status_pb2 import Status
from tensorlake.utils.retries import exponential_backoff

from indexify.executor.function_executor.function_executor import FunctionExecutor
from indexify.executor.function_executor.health_checker import HealthCheckResult
from indexify.executor.function_executor.server.function_executor_server import (
    FunctionExecutorServerStatus,
)
from indexify.executor.limits import (
    MAX_FUNCTION_CALL_EXECUTION_PLAN_UPDATE_ITEMS_COUNT,
    MAX_FUNCTION_CALL_SIZE_MB,
    SERVER_RPC_MAX_BACKOFF_SEC,
    SERVER_RPC_MAX_RETRIES,
    SERVER_RPC_MIN_BACKOFF_SEC,
    SERVER_RPC_TIMEOUT_SEC,
)
from indexify.proto.executor_api_pb2 import (
    AllocationFailureReason,
)
from indexify.proto.executor_api_pb2 import AllocationOutcomeCode
from indexify.proto.executor_api_pb2 import (
    AllocationOutcomeCode as ServerAllocationOutcomeCode,
)
from indexify.proto.executor_api_pb2 import (
    FunctionCallRequest as ServerFunctionCallRequest,
)
from indexify.proto.executor_api_pb2 import (
    FunctionCallResult as ServerFunctionCallResult,
)
from indexify.proto.executor_api_pb2 import FunctionCallWatch as ServerFunctionCallWatch
from indexify.proto.executor_api_pb2 import (
    FunctionExecutorTerminationReason,
)
from indexify.proto.executor_api_pb2_grpc import ExecutorAPIStub

from ..blob_store import BLOBMetadata, BLOBStore
from ..channel_manager import ChannelManager
from ..state_reporter import ExecutorStateReporter
from .aio_utils import shielded_await
from .allocation_info import AllocationInfo
from .allocation_output import AllocationOutput
from .blob_utils import (
    data_payload_to_serialized_object_inside_blob,
    presign_read_only_blob,
    presign_read_only_blob_for_data_payload,
    presign_write_only_blob,
)
from .events import AllocationExecutionFinished
from .execution_plan_updates import to_server_execution_plan_updates
from .function_call_watch_dispatcher import FunctionCallWatchDispatcher
from .metrics.allocation_runner import (
    metric_allocation_runner_allocation_run_errors,
    metric_allocation_runner_allocation_run_latency,
    metric_allocation_runner_allocation_runs,
    metric_allocation_runner_allocation_runs_in_progress,
    metric_call_function_rpc_errors,
    metric_call_function_rpc_latency,
    metric_call_function_rpcs,
    metric_function_call_message_size_mb,
)

# FE RPC timeouts
_CREATE_ALLOCATION_TIMEOUT_SECS = 5
_SEND_ALLOCATION_UPDATE_TIMEOUT_SECS = 5
_DELETE_ALLOCATION_TIMEOUT_SECS = 5


class _AllocationExecutionException(Exception):
    """Base class for allocation execution exceptions.

    Used internally in AllocationRunner.
    """


class _AllocationTimeoutError(_AllocationExecutionException):
    """Raised when allocation execution times out.

    Used internally in AllocationRunner.
    """


class _AllocationFailedLeavingFEInUndefinedState(_AllocationExecutionException):
    """Raised when allocation execution fails leaving FE in undefined state.

    Used internally in AllocationRunner.
    """


class _AllocationFailedDueToUserError(_AllocationExecutionException):
    """Raised when allocation execution fails due to user error.

    Used internally in AllocationRunner. Doesn't imply that FE is in undefined state.
    """


@dataclass
class _BLOBInfo:
    id: str
    uri: str
    upload_id: str


@dataclass
class _FunctionCallWatcherInfo:
    fe_watcher_id: str
    server_watch: ServerFunctionCallWatch
    # aio task that wait for the result or cancellation event to appear on the queue.
    task: asyncio.Task
    # Queue where state reconciler puts the function call watcher results.
    # Queue max size is 1.
    result_queue: asyncio.Queue
    result_sent_to_fe: asyncio.Event


@dataclass
class _RequestStateWriteOperationInfo:
    operation_id: str
    # None if creating BLOB failed.
    blob_info: _BLOBInfo | None


@dataclass
class _RunAllocationOnFunctionExecutorResult:
    """Represents the result of running allocation on Function Executor."""

    fe_result: FEAllocationResult
    execution_end_time: float
    # Not None if allocation uploaded its outputs into output BLOB.
    function_outputs_blob_uri: str | None


async def run_allocation_on_function_executor(
    alloc_info: AllocationInfo,
    function_executor: FunctionExecutor,
    blob_store: BLOBStore,
    state_reporter: ExecutorStateReporter,
    function_call_watch_dispatcher: FunctionCallWatchDispatcher,
    channel_manager: ChannelManager,
    logger: Any,
) -> AllocationExecutionFinished:
    """Runs the allocation on the Function Executor and sets alloc_info.output with the result.

    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)
    runner: AllocationRunner = AllocationRunner(
        alloc_info=alloc_info,
        function_executor=function_executor,
        blob_store=blob_store,
        state_reporter=state_reporter,
        function_call_watch_dispatcher=function_call_watch_dispatcher,
        channel_manager=channel_manager,
        logger=logger,
    )

    try:
        return await runner.run()
    finally:
        # We have to complete runner destroy before returning because FE Controller assumes
        # that no allocation aio tasks are running on return.
        await shielded_await(
            asyncio.create_task(
                runner.destroy(),
                name=f"allocation_runner_destroy:{alloc_info.allocation.allocation_id}",
            ),
            logger,
        )


class AllocationRunner:
    def __init__(
        self,
        alloc_info: AllocationInfo,
        function_executor: FunctionExecutor,
        blob_store: BLOBStore,
        state_reporter: ExecutorStateReporter,
        function_call_watch_dispatcher: FunctionCallWatchDispatcher,
        channel_manager: ChannelManager,
        logger: Any,
    ):
        """Initializes the AllocationRunner.

        Doesn't block, doesn't raise any exceptions.
        """
        self._alloc_info: AllocationInfo = alloc_info
        self._function_executor: FunctionExecutor = function_executor
        self._blob_store: BLOBStore = blob_store
        self._state_reporter: ExecutorStateReporter = state_reporter
        self._function_call_watch_dispatcher: FunctionCallWatchDispatcher = (
            function_call_watch_dispatcher
        )
        self._channel_manager: ChannelManager = channel_manager
        self._logger: Any = logger
        # Executor side allocation state:
        #
        # BLOB ID -> BLOBInfo
        self._pending_output_blobs: Dict[str, _BLOBInfo] = {}
        # Allocation Function Call IDs.
        self._pending_function_call_ids: Set[str] = set()
        # Allocation Function Call Watcher ID -> _FunctionCallWatcherInfo
        self._pending_function_call_watchers: Dict[str, _FunctionCallWatcherInfo] = {}
        # Allocation Request State Read Operation IDs
        self._pending_request_state_read_operations: set[str] = set()
        # Allocation Request State Write Operation ID -> _RequestStateWriteOperationInfo
        self._pending_request_state_write_operations: Dict[
            str, _RequestStateWriteOperationInfo
        ] = {}

    async def destroy(self) -> None:
        """Releases all the resources held by the AllocationRunner.

        Doesn't raise any exceptions. Idempotent.
        """
        while len(self._pending_output_blobs) > 0:
            try:
                write_op_info: _BLOBInfo = self._pending_output_blobs.popitem()[1]
                await self._blob_store.abort_multipart_upload(
                    uri=write_op_info.uri,
                    upload_id=write_op_info.upload_id,
                    logger=self._logger,
                )
            except BaseException as e:
                # Also catches any aio cancellations because it's important to clean up all resources.
                self._logger.error(
                    "failed to abort pending output blob multipart upload during AllocationRunner destruction",
                    blob_id=write_op_info.id,
                    exc_info=e,
                )

        while len(self._pending_request_state_write_operations) > 0:
            try:
                write_op_info: _RequestStateWriteOperationInfo = (
                    self._pending_request_state_write_operations.popitem()[1]
                )
                if write_op_info.blob_info is not None:
                    await self._blob_store.abort_multipart_upload(
                        uri=write_op_info.blob_info.uri,
                        upload_id=write_op_info.blob_info.upload_id,
                        logger=self._logger,
                    )
            except BaseException as e:
                # Also catches any aio cancellations because it's important to clean up all resources.
                self._logger.error(
                    "failed to abort pending request state write operation multipart upload during AllocationRunner destruction",
                    operation_id=write_op_info.operation_id,
                    blob_id=write_op_info.blob_info.id,
                    exc_info=e,
                )

        for watcher_info in list(self._pending_function_call_watchers.values()):
            try:
                await self._delete_function_call_watcher(watcher_info)
            except BaseException as e:
                # Also catches any aio cancellations because it's important to clean up all resources.
                self._logger.error(
                    "failed to cancel pending function call watcher during AllocationRunner destruction",
                    watcher=str(watcher_info.server_watch),
                    exc_info=e,
                )

    async def run(self) -> AllocationExecutionFinished:
        """Runs the allocation on the Function Executor and sets AllocationInfo.output.

        Doesn't raise any exceptions.
        """
        # Not None if the Function Executor should be terminated after running the alloc.
        function_executor_termination_reason: (
            FunctionExecutorTerminationReason | None
        ) = None

        # NB: We start this timer before invoking the first RPC, since
        # user code should be executing by the time the create_allocation() RPC
        # returns, so not attributing the allocation management RPC overhead to
        # the user would open a possibility for abuse. (This is somewhat
        # mitigated by the fact that these RPCs should have a very low
        # overhead.)
        execution_start_time: float = time.monotonic()

        try:
            with (
                metric_allocation_runner_allocation_runs_in_progress.track_inprogress(),
                metric_allocation_runner_allocation_run_errors.count_exceptions(),
            ):
                metric_allocation_runner_allocation_runs.inc()
                run_result: _RunAllocationOnFunctionExecutorResult = (
                    await self._run_allocation_on_fe()
                )
                self._alloc_info.output = AllocationOutput.from_allocation_result(
                    fe_result=run_result.fe_result,
                    function_outputs_blob_uri=run_result.function_outputs_blob_uri,
                    execution_duration_ms=_execution_duration(
                        execution_start_time, run_result.execution_end_time
                    ),
                    logger=self._logger,
                )
                if (
                    self._alloc_info.output.outcome_code
                    == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE
                ):
                    metric_allocation_runner_allocation_run_errors.inc()  # No exception, increment metric manually.
        except asyncio.CancelledError as e:
            self._logger.info(
                "allocation run was cancelled",
                exc_info=e,
            )
            # Allocation run was cancelled by FunctionExecutorController via aio task.cancel().
            # The allocation is still running in FE, we only cancelled the client-side RPC.
            function_executor_termination_reason = (
                FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_CANCELLED
            )
            self._alloc_info.is_cancelled = True
            self._alloc_info.output = AllocationOutput.allocation_cancelled(
                execution_duration_ms=_execution_duration(
                    execution_start_time, time.monotonic()
                ),
            )
        except _AllocationTimeoutError as e:
            self._logger.info(
                "allocation run timed out",
                exc_info=e,
            )
            function_executor_termination_reason = (
                FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_TIMEOUT
            )
            self._alloc_info.output = AllocationOutput.function_timeout(
                execution_duration_ms=_execution_duration(
                    execution_start_time, time.monotonic()
                ),
            )
        except _AllocationFailedDueToUserError as e:
            self._logger.info(
                "allocation run failed due to user error",
                exc_info=e,
            )
            self._alloc_info.output = (
                AllocationOutput.function_error_with_healthy_function_executor(
                    execution_duration_ms=_execution_duration(
                        execution_start_time, time.monotonic()
                    ),
                )
            )
        except _AllocationFailedLeavingFEInUndefinedState as e:
            self._logger.info(
                "allocation run failed leaving FE in undefined state",
                exc_info=e,
            )
            # We attribute all failures here to user errors and charge user for the execution time.
            # If we do the opposite this will enable service abuse.
            # Try to determine cause of the failure for better reporting.
            try:
                server_status: FunctionExecutorServerStatus = (
                    await self._function_executor.server_status()
                )
            except asyncio.CancelledError:
                # Our aio task was cancelled while we were determining FE bad state.
                # We should not keep blocking the cancellation with awaits, continue without full FE info.
                server_status: FunctionExecutorServerStatus = (
                    FunctionExecutorServerStatus.healthy()
                )

            if server_status.oom_killed:
                function_executor_termination_reason = (
                    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_OOM
                )
                self._alloc_info.output = AllocationOutput.allocation_ran_out_of_memory(
                    execution_duration_ms=_execution_duration(
                        execution_start_time, time.monotonic()
                    ),
                )
                self._logger.info(
                    "Function Executor was OOM killed while running the allocation, shutting down Function Executor"
                )
            else:
                function_executor_termination_reason = (
                    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_UNHEALTHY
                )
                self._alloc_info.output = AllocationOutput.function_executor_is_in_undefined_state_after_running_allocation(
                    execution_duration_ms=_execution_duration(
                        execution_start_time, time.monotonic()
                    ),
                )
                self._logger.info(
                    "Function Executor is in undefined state after running the allocation, shutting down Function Executor"
                )
        except BaseException as e:
            # This is an unexpected exception in our Executor code; we believe that this indicates an internal error.
            # We still charge the user to prevent service abuse scenarios (like a DDOS atack on Executor).
            self._logger.error(
                "unexpected internal error during allocation execution",
                exc_info=e,
            )
            self._alloc_info.output = AllocationOutput.internal_error(
                execution_duration_ms=_execution_duration(
                    execution_start_time, time.monotonic()
                ),
            )

        metric_allocation_runner_allocation_run_latency.observe(
            time.monotonic() - execution_start_time
        )

        # Check if the allocation failed because the FE is unhealthy to prevent more allocations failing.
        if (
            self._alloc_info.output.outcome_code
            == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE
            and function_executor_termination_reason is None
        ):
            try:
                result: HealthCheckResult = (
                    await self._function_executor.health_checker().check()
                )
            except asyncio.CancelledError:
                # Our aio task was cancelled while we were determining FE bad state.
                # We should not keep blocking the cancellation with awaits, continue without full FE info.
                result: HealthCheckResult = HealthCheckResult.healthy()

            # The FE can't be OOM killed because this would trigger _AllocationFailedLeavingFEInUndefinedState exception
            # because if FE got OOM killed then we'll never get AllocationResult from FE.
            if not result.is_healthy:
                function_executor_termination_reason = (
                    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_UNHEALTHY
                )
                self._logger.error(
                    "Function Executor health check failed after running allocation, shutting down Function Executor",
                    health_check_fail_reason=result.reason,
                    function_executor_termination_reason=function_executor_termination_reason,
                )

        _log_alloc_execution_finished(
            output=self._alloc_info.output, logger=self._logger
        )

        return AllocationExecutionFinished(
            alloc_info=self._alloc_info,
            function_executor_termination_reason=function_executor_termination_reason,
        )

    async def _run_allocation_on_fe(self) -> _RunAllocationOnFunctionExecutorResult:
        """Runs the allocation on the Function Executor via RPCs.

        In case of allocation failure raises either _AllocationTimeoutError or
        _AllocationFailedLeavingFEInUndefinedState. Raises an Exception if an
        internal error occured on Executor side.
        """
        fe_alloc: FEAllocation = FEAllocation(
            request_id=self._alloc_info.allocation.request_id,
            function_call_id=self._alloc_info.allocation.function_call_id,
            allocation_id=self._alloc_info.allocation.allocation_id,
            inputs=self._alloc_info.input.function_inputs,
        )

        fe_channel: grpc.aio.Channel | None = self._function_executor.channel()
        if fe_channel is None:
            raise asyncio.CancelledError()
        fe_stub = FunctionExecutorStub(fe_channel)

        # Exception handling logic specific to this function.
        def _raise_from_grpc_error(e: grpc.aio.AioRpcError) -> None:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise _AllocationTimeoutError() from e
            else:
                raise _AllocationFailedLeavingFEInUndefinedState() from e

        try:
            await fe_stub.create_allocation(
                FECreateAllocationRequest(allocation=fe_alloc),
                timeout=_CREATE_ALLOCATION_TIMEOUT_SECS,
            )
        except grpc.aio.AioRpcError as e:
            _raise_from_grpc_error(e)

        try:
            # This aio call is non-blocking, so no timeout needed for establishing
            # the stream. Timeout for each individual read() is set below.
            watch_allocation_state_rpc_stream = fe_stub.watch_allocation_state(
                FEWatchAllocationStateRequest(
                    allocation_id=self._alloc_info.allocation.allocation_id
                ),
            )
        except grpc.aio.AioRpcError as e:
            _raise_from_grpc_error(e)

        # Run reconciliation loop for the Allocation states stream.
        previous_progress: FEAllocationProgress | None = None
        allocation_result: FEAllocationResult | None = None

        function_timeout_sec: float = self._alloc_info.allocation_timeout_ms / 1000.0
        dynamic_deadline_sec: float = time.monotonic() + function_timeout_sec
        try:
            while True:
                # allocation_state_read_task is automatically cancelled in finally block below
                # when we cancel the watch_allocation_state_rpc_stream.
                new_allocation_state_read_task: asyncio.Task = asyncio.create_task(
                    watch_allocation_state_rpc_stream.read(),
                    name=f"allocation_state_read_wait:{self._alloc_info.allocation.allocation_id}",
                )

                dynamic_deadline_sec, new_alloc_state = (
                    await self._wait_new_allocation_state(
                        new_allocation_state_read_task=new_allocation_state_read_task,
                        dynamic_deadline_sec=dynamic_deadline_sec,
                    )
                )
                new_alloc_state: FEAllocationState | None

                if new_alloc_state is None:
                    # FE ended the stream without sending AllocationResult, FE is in undefined state.
                    allocation_result = FEAllocationResult(
                        outcome_code=FEAllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                        failure_reason=FEAllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR,
                    )
                    break

                dynamic_deadline_sec, previous_progress = (
                    await self._reconcile_fe_allocation_state(
                        fe_stub=fe_stub,
                        state=new_alloc_state,
                        current_dynamic_deadline_sec=dynamic_deadline_sec,
                        current_progress=previous_progress,
                    )
                )

                # Check for allocation result last to make sure we reconciled the last state.
                if new_alloc_state.HasField("result"):
                    allocation_result = new_alloc_state.result
                    break
        except grpc.aio.AioRpcError as e:
            # This is regular FE gRPC call error (all Server gRPC calls errors are handled in-place).
            _raise_from_grpc_error(e)
        except _AllocationExecutionException:
            raise
        except Exception as e:
            # We cannot rollback FE state to something clean so we have to report leaving FE in undefined state here.
            self._logger.error(
                "failed to execute allocation",
                exc_info=e,
            )
            raise _AllocationFailedLeavingFEInUndefinedState() from e
        finally:
            # Cancel the outstanding RPC to ensure any resources in use are cleaned up;
            # This is idempotent (in case the RPC has already completed).
            watch_allocation_state_rpc_stream.cancel()

        try:
            await fe_stub.delete_allocation(
                FEDeleteAllocationRequest(
                    allocation_id=self._alloc_info.allocation.allocation_id
                ),
                timeout=_DELETE_ALLOCATION_TIMEOUT_SECS,
            )
        except grpc.aio.AioRpcError as e:
            _raise_from_grpc_error(e)

        # Not billing the user for the rest of this function duration.
        execution_end_time: float = time.monotonic()

        function_outputs_blob_uri: str | None = None
        if allocation_result.HasField("uploaded_function_outputs_blob"):
            blob_info: _BLOBInfo | None = self._pending_output_blobs.get(
                allocation_result.uploaded_function_outputs_blob.id
            )
            if blob_info is None:
                # Either output blob upload is already completed or FE sent an invalid blob ID.
                # In both cases this is FE (user) error.
                self._logger.info(
                    "failing allocation because its outputs blob is not found",
                    output_blob_id=allocation_result.uploaded_function_outputs_blob.id,
                )
                raise _AllocationFailedDueToUserError()

            function_outputs_blob_uri = blob_info.uri
            # Any failure here is internal error, so no need to catch exceptions.
            # aio cancellation will also propagate to caller.
            await self._complete_blob_upload(
                blob_info, allocation_result.uploaded_function_outputs_blob
            )

            if allocation_result.HasField("updates"):
                if (
                    allocation_result.updates.ByteSize()
                    > MAX_FUNCTION_CALL_SIZE_MB * 1024 * 1024
                ):
                    self._logger.info(
                        "failing allocation because its FE execution plan updates bytes size exceeds maximum",
                        message_size_bytes=allocation_result.updates.ByteSize(),
                        max_message_size_bytes=MAX_FUNCTION_CALL_SIZE_MB * 1024 * 1024,
                    )
                    raise _AllocationFailedDueToUserError()

                if (
                    len(allocation_result.updates.updates)
                    > MAX_FUNCTION_CALL_EXECUTION_PLAN_UPDATE_ITEMS_COUNT
                ):
                    self._logger.info(
                        "failing allocation because its FE execution plan updates items count exceeds maximum",
                        items_count=len(allocation_result.updates.updates),
                        max_items_count=MAX_FUNCTION_CALL_EXECUTION_PLAN_UPDATE_ITEMS_COUNT,
                    )
                    raise _AllocationFailedDueToUserError()

                # Validate FE updates by converting them to Server Updates. This is inefficient but works.
                try:
                    to_server_execution_plan_updates(
                        allocation_result.updates, function_outputs_blob_uri
                    )
                except Exception as e:
                    self._logger.info(
                        "failing allocation because its FE execution plan updates are invalid",
                        exc_info=e,
                    )
                    raise _AllocationFailedDueToUserError()

        return _RunAllocationOnFunctionExecutorResult(
            fe_result=allocation_result,
            execution_end_time=execution_end_time,
            function_outputs_blob_uri=function_outputs_blob_uri,
        )

    async def _wait_new_allocation_state(
        self, new_allocation_state_read_task: asyncio.Task, dynamic_deadline_sec: float
    ) -> tuple[float, FEAllocationState | None]:
        """Waits for a new allocation state to arrive on allocation_state_read_task or for allocation timeout to occur.

        Returns a tuple of (new dynamic deadline, new allocation state).
        Raises _AllocationTimeoutError if allocation timed out.
        Raises an Exception on internal errors.
        """
        while not new_allocation_state_read_task.done():
            wait_tasks: list[asyncio.Task] = [new_allocation_state_read_task]

            pending_function_call_watcher: _FunctionCallWatcherInfo | None = None
            for watcher_info in self._pending_function_call_watchers.values():
                if not watcher_info.result_sent_to_fe.is_set():
                    pending_function_call_watcher = watcher_info
                    break

            allocation_timeout_task: asyncio.Task | None = None
            function_call_watcher_done_task: asyncio.Task | None = None
            if pending_function_call_watcher is None:
                allocation_timeout_task = asyncio.create_task(
                    asyncio.sleep(dynamic_deadline_sec - time.monotonic())
                )
                wait_tasks.append(allocation_timeout_task)
            else:
                # A pending function call watcher means that user code in FE is blocked on a
                # synchronous function call or a Future result. The watcher function can use
                # progress updates or might be slowed down due to i.e. Indexify cluster slowness.
                # This is why we don't know when the watcher function would finish so we don't
                # apply the dynamic timeout while a watcher is pending.
                function_call_watcher_done_task = asyncio.create_task(
                    pending_function_call_watcher.result_sent_to_fe.wait(),
                    name=f"function_call_watcher_done_wait:{self._alloc_info.allocation.allocation_id}:{pending_function_call_watcher.fe_watcher_id}",
                )
                wait_tasks.append(function_call_watcher_done_task)
                self._logger.info(
                    "waiting for function call watcher, function timeout not applied",
                    watcher_id=pending_function_call_watcher.fe_watcher_id,
                    child_fn_call_id=pending_function_call_watcher.server_watch.function_call_id,
                )

            wait_start_sec: float = time.monotonic()
            try:
                done, _ = await asyncio.wait(
                    wait_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if (
                    allocation_timeout_task is not None
                    and allocation_timeout_task in done
                ):
                    raise _AllocationTimeoutError()
            finally:
                # Cancel all aio tasks to avoid resource leaks.
                if allocation_timeout_task is not None:
                    allocation_timeout_task.cancel()
                    allocation_timeout_task = None
                if function_call_watcher_done_task is not None:
                    function_call_watcher_done_task.cancel()
                    function_call_watcher_done_task = None

            if pending_function_call_watcher is not None:
                self._logger.info(
                    "done waiting for function call watcher",
                    watcher_id=pending_function_call_watcher.fe_watcher_id,
                    child_fn_call_id=pending_function_call_watcher.server_watch.function_call_id,
                    is_watcher_complete=pending_function_call_watcher.result_sent_to_fe.is_set(),
                    duration_sec=time.monotonic() - wait_start_sec,
                )

                # Waiting for function call watcher completion is not counted against function timeout.
                dynamic_deadline_sec += time.monotonic() - wait_start_sec

        new_allocation_state: FEAllocationState | grpc.aio.EOF = (
            new_allocation_state_read_task.result()
        )
        return dynamic_deadline_sec, (
            None if new_allocation_state == grpc.aio.EOF else new_allocation_state
        )

    async def _reconcile_fe_allocation_state(
        self,
        fe_stub: FunctionExecutorStub,
        state: FEAllocationState,
        current_dynamic_deadline_sec: float,
        current_progress: FEAllocationProgress | None,
    ) -> tuple[float, FEAllocationProgress | None]:
        """Reconciles the provided FE allocation state.

        Returns updated dynamic deadline for the allocation and new progress message if any.
        raises an Exception on internal Executor or FE error.
        """
        # The dynamic deadline is equal to function timeout and gets extended by:
        # * each new progress message (aka deadline extension initiated by user),
        # * Executor side latency that adds to function duration because it typically
        #   blocks the user code i.e. (commiting a BLOB write operation with S3,
        #   creating Function Calls on Server side). These operations are calling
        #   external service with a retry policy which allows minutes of retries.
        function_timeout_sec: float = self._alloc_info.allocation_timeout_ms / 1000.0

        reconciliation_start_time_sec: float = time.monotonic()
        await self._reconcile_output_blobs(fe_stub, state.output_blob_requests)
        await self._reconcile_function_calls(fe_stub, state.function_calls)
        await self._reconcile_function_call_watchers(
            fe_stub, state.function_call_watchers
        )
        await self._reconcile_request_state_operations(
            fe_stub, state.request_state_operations
        )
        reconciliation_duration_sec: float = (
            time.monotonic() - reconciliation_start_time_sec
        )
        current_dynamic_deadline_sec += reconciliation_duration_sec
        if reconciliation_duration_sec > 1.0:
            self._logger.info(
                "allocation state reconciliation took more than 1 second, function deadline extended",
                duration_sec=reconciliation_duration_sec,
            )

        if state.HasField("progress"):
            if current_progress != state.progress:
                current_dynamic_deadline_sec = time.monotonic() + function_timeout_sec
            current_progress = state.progress

        return current_dynamic_deadline_sec, current_progress

    async def _reconcile_output_blobs(
        self,
        fe_stub: FunctionExecutorStub,
        output_blob_requests: list[FEAllocationOutputBLOBRequest],
    ) -> None:
        fe_output_blob_requests: Dict[str, FEAllocationOutputBLOBRequest] = {}
        for output_blob_request in output_blob_requests:
            if not _validate_fe_output_blob_request(output_blob_request):
                self._logger.warning(
                    "skipping invalid FE output BLOB request",
                    output_blob_request=str(output_blob_request),
                )
                continue
            fe_output_blob_requests[output_blob_request.id] = output_blob_request

            if output_blob_request.id not in self._pending_output_blobs:
                update: FEAllocationUpdate = FEAllocationUpdate(
                    allocation_id=self._alloc_info.allocation.allocation_id,
                )
                try:
                    output_blob: FEBLOB = await self._create_output_blob(
                        output_blob_request
                    )
                    update.output_blob.CopyFrom(
                        FEAllocationOutputBLOB(
                            status=Status(
                                code=grpc.StatusCode.OK.value[0],
                            ),
                            blob=output_blob,
                        )
                    )
                except Exception as e:
                    self._logger.error(
                        "failed to create output BLOB for FE output BLOB request",
                        blob_id=output_blob_request.id,
                        exc_info=e,
                    )
                    update.output_blob.CopyFrom(
                        FEAllocationOutputBLOB(
                            status=Status(
                                code=grpc.StatusCode.INTERNAL.value[0],
                                message="Failed to create output BLOB.",
                            ),
                            blob=FEBLOB(id=output_blob_request.id),
                        )
                    )

                await fe_stub.send_allocation_update(
                    update,
                    timeout=_SEND_ALLOCATION_UPDATE_TIMEOUT_SECS,
                )

        # When user deletes blob request we should not delete the blob because
        # blob is separate from its request. The blob will be deleted when we
        # finalize its upload.

    async def _create_output_blob(
        self, fe_output_blob_request: FEAllocationOutputBLOBRequest
    ) -> FEBLOB:
        """Creates an output BLOB for the given FE output BLOB request.

        Raises an Exception if creation fails.
        """
        blob_uri: str = (
            f"{self._alloc_info.allocation.request_data_payload_uri_prefix}.{self._alloc_info.allocation.allocation_id}.{fe_output_blob_request.id}.output"
        )
        function_outputs_blob_upload_id: str = (
            await self._blob_store.create_multipart_upload(
                uri=blob_uri,
                logger=self._logger,
            )
        )
        blob_info: _BLOBInfo = _BLOBInfo(
            id=fe_output_blob_request.id,
            uri=blob_uri,
            upload_id=function_outputs_blob_upload_id,
        )
        # If any failure occures then this blob upload will be aborted in destroy().
        self._pending_output_blobs[fe_output_blob_request.id] = blob_info

        return await presign_write_only_blob(
            blob_id=blob_info.id,
            blob_uri=blob_info.uri,
            upload_id=blob_info.upload_id,
            size=fe_output_blob_request.size,
            blob_store=self._blob_store,
            logger=self._logger,
        )

    async def _complete_blob_upload(
        self, blob_info: _BLOBInfo, uploaded_blob: FEBLOB
    ) -> None:
        await self._blob_store.complete_multipart_upload(
            uri=blob_info.uri,
            upload_id=blob_info.upload_id,
            parts_etags=[blob_chunk.etag for blob_chunk in uploaded_blob.chunks],
            logger=self._logger,
        )
        del self._pending_output_blobs[uploaded_blob.id]

    async def _reconcile_function_calls(
        self,
        fe_stub: FunctionExecutorStub,
        function_calls: list[FEAllocationFunctionCall],
    ) -> None:
        fe_function_call_ids: Set[str] = set()
        for function_call in function_calls:
            # TODO: Remove updates.root_function_call_id after FE migration period.
            function_call_id: str = (
                function_call.id
                if _proto_field_is_defined(function_call, "id")
                and function_call.HasField("id")
                else function_call.updates.root_function_call_id
            )

            if not _validate_fe_function_call(function_call):
                self._logger.warning(
                    "skipping invalid FE function call",
                    alloc_fn_call_id=function_call_id,
                )
                continue

            fe_function_call_ids.add(function_call_id)
            if function_call_id not in self._pending_function_call_ids:
                self._pending_function_call_ids.add(function_call_id)

                try:
                    await self._create_function_call(function_call)
                    await self._send_function_call_creation_result_to_fe(
                        fe_stub=fe_stub,
                        allocation_id=self._alloc_info.allocation.allocation_id,
                        allocation_function_call_id=function_call_id,
                        result=FEAllocationFunctionCallCreationResult(
                            status=Status(
                                code=grpc.StatusCode.OK.value[0],
                            ),
                        ),
                    )
                except Exception as e:
                    self._logger.error(
                        "failed to create function call",
                        alloc_fn_call_id=function_call_id,
                        child_fn_call_id=function_call.updates.root_function_call_id,
                        exc_info=e,
                    )
                    await self._send_function_call_creation_result_to_fe(
                        fe_stub=fe_stub,
                        allocation_id=self._alloc_info.allocation.allocation_id,
                        allocation_function_call_id=function_call_id,
                        result=FEAllocationFunctionCallCreationResult(
                            status=Status(
                                code=grpc.StatusCode.INTERNAL.value[0],
                                message="Failed to create function call.",
                            ),
                        ),
                    )

        for function_call_id in list(self._pending_function_call_ids):
            if function_call_id not in fe_function_call_ids:
                self._pending_function_call_ids.remove(function_call_id)

    async def _send_function_call_creation_result_to_fe(
        self,
        fe_stub: FunctionExecutorStub,
        allocation_id: str,
        allocation_function_call_id: str,
        result: FEAllocationFunctionCallCreationResult,
    ) -> None:
        """Sends function call creation result to FE.

        Doesn't raise any exceptions.
        """
        # TODO: function_call_id is deprecated, remove after FE migration period.
        if _proto_field_is_defined(result, "function_call_id"):
            result.function_call_id = allocation_function_call_id
        if _proto_field_is_defined(result, "allocation_function_call_id"):
            result.allocation_function_call_id = allocation_function_call_id

        try:
            await fe_stub.send_allocation_update(
                FEAllocationUpdate(
                    allocation_id=allocation_id,
                    function_call_creation_result=result,
                ),
                timeout=_SEND_ALLOCATION_UPDATE_TIMEOUT_SECS,
            )
        except grpc.aio.AioRpcError as e:
            # Ignore this error code because older FE versions don't wait for this update. They
            # fail with FAILED_PRECONDITION when they already finished running the alloc.
            # TODO: Remove this workaround once all FE versions wait for this update.
            if e.code() != grpc.StatusCode.FAILED_PRECONDITION:
                raise

    async def _create_function_call(
        self, fe_function_call: FEAllocationFunctionCall
    ) -> None:
        """Creates a function call in the server.

        Raises exception if the creation fails.
        """
        blob_info: _BLOBInfo | None = None
        if fe_function_call.HasField("args_blob"):
            blob_info = self._pending_output_blobs.get(fe_function_call.args_blob.id)
            if blob_info is None:
                raise ValueError(
                    f"Args blob with id {fe_function_call.args_blob.id} is not found on Executor side"
                )

        if blob_info is not None:
            await self._complete_blob_upload(blob_info, fe_function_call.args_blob)

        server_function_call_request: ServerFunctionCallRequest = (
            ServerFunctionCallRequest(
                namespace=self._alloc_info.allocation.function.namespace,
                application=self._alloc_info.allocation.function.application_name,
                request_id=self._alloc_info.allocation.request_id,
                source_function_call_id=self._alloc_info.allocation.function_call_id,
                updates=to_server_execution_plan_updates(
                    fe_execution_plan_updates=fe_function_call.updates,
                    args_blob_uri=None if blob_info is None else blob_info.uri,
                ),
            )
        )

        request_size_mb: float = server_function_call_request.ByteSize() / (
            1024.0 * 1024.0
        )
        metric_function_call_message_size_mb.observe(request_size_mb)
        if request_size_mb > MAX_FUNCTION_CALL_SIZE_MB:
            raise ValueError(
                f"Function call message size {request_size_mb} MB exceeds maximum of {MAX_FUNCTION_CALL_SIZE_MB} MB"
            )

        if (
            len(server_function_call_request.updates.updates)
            > MAX_FUNCTION_CALL_EXECUTION_PLAN_UPDATE_ITEMS_COUNT
        ):
            raise ValueError(
                f"Function call execution plan update items count {len(server_function_call_request.updates.updates)} exceeds maximum of {MAX_FUNCTION_CALL_EXECUTION_PLAN_UPDATE_ITEMS_COUNT}"
            )

        @exponential_backoff(
            max_retries=SERVER_RPC_MAX_RETRIES,
            initial_delay_seconds=SERVER_RPC_MIN_BACKOFF_SEC,
            max_delay_seconds=SERVER_RPC_MAX_BACKOFF_SEC,
            retryable_exceptions=(grpc.aio.AioRpcError,),
            jitter_range=(0.5, 1.5),
            on_retry=lambda exc, delay_sec, attempt: self._logger.error(
                f"call_function RPC failed, retrying in {delay_sec} seconds",
                attempt=attempt,
                exc_info=exc,
            ),
        )
        async def _call_function_rpc() -> None:
            with (
                metric_call_function_rpc_errors.count_exceptions(),
                metric_call_function_rpc_latency.time(),
            ):
                metric_call_function_rpcs.inc()
                ExecutorAPIStub(
                    await self._channel_manager.get_shared_channel()
                ).call_function(
                    server_function_call_request,
                    timeout=SERVER_RPC_TIMEOUT_SEC,
                )

        try:
            await _call_function_rpc()
        except grpc.aio.AioRpcError as e:
            raise RuntimeError(
                "Server call_function RPC failed after all retries"
            ) from e

    async def _reconcile_function_call_watchers(
        self,
        fe_stub: FunctionExecutorStub,
        function_call_watchers: list[FEAllocationFunctionCallWatcher],
    ) -> None:
        fe_function_call_watchers: Dict[str, FEAllocationFunctionCallWatcher] = {}
        for function_call_watcher in function_call_watchers:
            if not _validate_fe_function_call_watcher(function_call_watcher):
                self._logger.warning(
                    "skipping invalid FE function call watcher",
                    function_call_watcher=str(function_call_watcher),
                )
                continue

            # TODO: Remove watcher_id field support after FE migration period.
            watcher_id: str = (
                function_call_watcher.id
                if _proto_field_is_defined(function_call_watcher, "id")
                and function_call_watcher.HasField("id")
                else function_call_watcher.watcher_id
            )
            fe_function_call_watchers[watcher_id] = function_call_watcher

            if watcher_id not in self._pending_function_call_watchers:
                # We allow allocation to watch function calls that were not started by it.
                # There's no known reason to disallow it and it simplifies implementation.
                await self._create_function_call_watcher(
                    watcher_id, function_call_watcher
                )

        for function_call_watcher_id in list(
            self._pending_function_call_watchers.keys()
        ):
            if function_call_watcher_id not in fe_function_call_watchers:
                await self._delete_function_call_watcher(
                    self._pending_function_call_watchers[function_call_watcher_id]
                )

    async def _create_function_call_watcher(
        self, watcher_id: str, fe_function_call_watcher: FEAllocationFunctionCallWatcher
    ) -> None:
        """Adds a note to Executor state to create a function call watcher.

        Server creates the watcher asynchronously based on the note.
        Doesn't raise any exceptions."""
        # TODO: remove function_call_id field support after FE migration period.
        watched_function_call_id: str = (
            fe_function_call_watcher.root_function_call_id
            if _proto_field_is_defined(
                fe_function_call_watcher, "root_function_call_id"
            )
            and fe_function_call_watcher.HasField("root_function_call_id")
            else fe_function_call_watcher.function_call_id
        )
        watcher_info: _FunctionCallWatcherInfo = _FunctionCallWatcherInfo(
            fe_watcher_id=watcher_id,
            server_watch=ServerFunctionCallWatch(
                namespace=self._alloc_info.allocation.function.namespace,
                application=self._alloc_info.allocation.function.application_name,
                request_id=self._alloc_info.allocation.request_id,
                function_call_id=watched_function_call_id,
            ),
            task=None,
            result_queue=asyncio.Queue(maxsize=1),
            result_sent_to_fe=asyncio.Event(),
        )
        watcher_info.task = asyncio.create_task(
            self._function_call_watcher_worker(watcher_info),
            name=f"function_call_result_watcher:{watcher_info.fe_watcher_id}",
        )
        self._pending_function_call_watchers[watcher_info.fe_watcher_id] = watcher_info

        self._function_call_watch_dispatcher.add_function_call_watch(
            watch=watcher_info.server_watch,
            result_queue=watcher_info.result_queue,
        )
        self._state_reporter.add_function_call_watcher(watcher_info.server_watch)
        self._state_reporter.schedule_state_report()

    async def _function_call_watcher_worker(
        self, watcher_info: _FunctionCallWatcherInfo
    ) -> None:
        """Waits for function call result or cancellation event and sends it to FE.

        If sending the result to FE fails then doesn't do anything."""
        try:
            function_call_result: ServerFunctionCallResult = (
                await watcher_info.result_queue.get()
            )

            # Check for any bugs.
            if (
                function_call_result.namespace
                != self._alloc_info.allocation.function.namespace
            ):
                raise ValueError(
                    "function call result namespace doesn't match allocation namespace: "
                    f"{function_call_result.namespace} != {self._alloc_info.allocation.function.namespace}"
                )
            if (
                function_call_result.request_id
                != self._alloc_info.allocation.request_id
            ):
                raise ValueError(
                    "function call result request_id doesn't match allocation request_id: "
                    f"{function_call_result.request_id} != {self._alloc_info.allocation.request_id}"
                )
            if (
                function_call_result.function_call_id
                != watcher_info.server_watch.function_call_id
            ):
                raise ValueError(
                    "function call result function_call_id doesn't match watcher function_call_id: "
                    f"{function_call_result.function_call_id} != {watcher_info.server_watch.function_call_id}"
                )

            fe_function_call_result: FEAllocationFunctionCallResult = (
                FEAllocationFunctionCallResult(
                    function_call_id=watcher_info.server_watch.function_call_id,
                    outcome_code=_to_fe_allocation_outcome_code(
                        function_call_result.outcome_code
                    ),
                )
            )
            if _proto_field_is_defined(fe_function_call_result, "watcher_id"):
                fe_function_call_result.watcher_id = watcher_info.fe_watcher_id

            if function_call_result.HasField("return_value"):
                fe_function_call_result.value_output.CopyFrom(
                    data_payload_to_serialized_object_inside_blob(
                        function_call_result.return_value,
                    )
                )
                fe_function_call_result.value_blob.CopyFrom(
                    await presign_read_only_blob_for_data_payload(
                        data_payload=function_call_result.return_value,
                        blob_store=self._blob_store,
                        logger=self._logger,
                    )
                )

            if function_call_result.HasField("request_error"):
                fe_function_call_result.request_error_output.CopyFrom(
                    data_payload_to_serialized_object_inside_blob(
                        function_call_result.request_error,
                    )
                )
                fe_function_call_result.request_error_blob.CopyFrom(
                    await presign_read_only_blob_for_data_payload(
                        data_payload=function_call_result.request_error,
                        blob_store=self._blob_store,
                        logger=self._logger,
                    )
                )

            # NB: aio cancel() can be called on us but we won't get the aio.CancelledError until we await.
            # So we have to check if FE is still alive here.

            # If we fail to send the result to FE we're not currently stopping the allocation execution.
            # This leaves FE in undefined state. Allocation timeout should handle that.
            fe_channel: grpc.aio.Channel | None = self._function_executor.channel()
            if fe_channel is None:
                raise asyncio.CancelledError()
            fe_stub = FunctionExecutorStub(fe_channel)

            await fe_stub.send_allocation_update(
                FEAllocationUpdate(
                    allocation_id=self._alloc_info.allocation.allocation_id,
                    function_call_result=fe_function_call_result,
                ),
                timeout=_SEND_ALLOCATION_UPDATE_TIMEOUT_SECS,
            )
        except asyncio.CancelledError:
            pass  # Expected during cancellation.
        except Exception as e:
            self._logger.error(
                "failed to send function call result to FE",
                exc_info=e,
                watch=str(watcher_info.server_watch),
            )
        finally:
            watcher_info.result_sent_to_fe.set()

    async def _delete_function_call_watcher(
        self, watcher_info: _FunctionCallWatcherInfo
    ) -> None:
        """Deletes a function call watcher in the server.

        If the deletion fails then doesn't do anything."""
        watcher_info.task.cancel()
        try:
            await watcher_info.task
        except asyncio.CancelledError:
            pass  # Expected.

        self._state_reporter.remove_function_call_watcher(watcher_info.server_watch)
        self._state_reporter.schedule_state_report()

        self._function_call_watch_dispatcher.remove_function_call_watch(
            watch=watcher_info.server_watch,
            result_queue=watcher_info.result_queue,
        )
        del self._pending_function_call_watchers[watcher_info.fe_watcher_id]

    async def _reconcile_request_state_operations(
        self,
        fe_stub: FunctionExecutorStub,
        request_state_operations: list[FEAllocationRequestStateOperation],
    ) -> None:
        await self._reconcile_request_state_read_operations(
            fe_stub, request_state_operations
        )
        await self._reconcile_request_state_write_operations(
            fe_stub, request_state_operations
        )

    async def _reconcile_request_state_read_operations(
        self,
        fe_stub: FunctionExecutorStub,
        request_state_operations: list[FEAllocationRequestStateOperation],
    ) -> None:
        fe_read_operation_ids: set[str] = set()
        for request_state_operation in request_state_operations:
            request_state_operation: FEAllocationRequestStateOperation
            if not request_state_operation.HasField("prepare_read"):
                continue

            fe_read_operation_ids.add(request_state_operation.operation_id)
            if (
                request_state_operation.operation_id
                not in self._pending_request_state_read_operations
            ):
                self._pending_request_state_read_operations.add(
                    request_state_operation.operation_id
                )
                await self._prepare_read_operation(fe_stub, request_state_operation)

        # Deletes read operations deleted by FE.
        self._pending_request_state_read_operations = fe_read_operation_ids

    async def _prepare_read_operation(
        self,
        fe_stub: FunctionExecutorStub,
        request_state_operation: FEAllocationRequestStateOperation,
    ) -> None:
        alloc_update: FEAllocationUpdate = FEAllocationUpdate(
            allocation_id=self._alloc_info.allocation.allocation_id,
            request_state_operation_result=FEAllocationRequestStateOperationResult(
                operation_id=request_state_operation.operation_id,
                prepare_read=FEAllocationRequestStatePrepareReadOperationResult(),
            ),
        )

        blob_uri: str = self._request_state_key_blob_uri(
            request_state_operation.state_key
        )
        try:
            blob_metadata: BLOBMetadata = await self._blob_store.get_metadata(
                blob_uri, self._logger
            )
            blob: FEBLOB = await presign_read_only_blob(
                blob_uri=blob_uri,
                blob_size=blob_metadata.size_bytes,
                blob_store=self._blob_store,
                logger=self._logger,
            )
            alloc_update.request_state_operation_result.status.code = (
                grpc.StatusCode.OK.value[0]
            )
            alloc_update.request_state_operation_result.prepare_read.blob.CopyFrom(blob)
        except KeyError:
            # BLOB not found, this is expected if user din't call request state set first.
            alloc_update.request_state_operation_result.status.code = (
                grpc.StatusCode.NOT_FOUND.value[0]
            )
        except Exception as e:
            self._logger.error(
                "failed to prepare request state read operation",
                exc_info=e,
                operation_id=request_state_operation.operation_id,
            )
            alloc_update.request_state_operation_result.status.code = (
                grpc.StatusCode.INTERNAL.value[0]
            )

        await fe_stub.send_allocation_update(
            alloc_update, timeout=_SEND_ALLOCATION_UPDATE_TIMEOUT_SECS
        )

    async def _reconcile_request_state_write_operations(
        self,
        fe_stub: FunctionExecutorStub,
        request_state_operations: list[FEAllocationRequestStateOperation],
    ) -> None:
        for request_state_operation in request_state_operations:
            request_state_operation: FEAllocationRequestStateOperation
            if request_state_operation.HasField("prepare_write"):
                if (
                    request_state_operation.operation_id
                    not in self._pending_request_state_write_operations
                ):
                    await self._prepare_request_state_write_operation(
                        fe_stub, request_state_operation
                    )
            elif request_state_operation.HasField("commit_write"):
                await self._reconcile_request_state_write_operation(
                    fe_stub, request_state_operation
                )

    async def _prepare_request_state_write_operation(
        self,
        fe_stub: FunctionExecutorStub,
        request_state_operation: FEAllocationRequestStateOperation,
    ) -> None:
        alloc_update: FEAllocationUpdate = FEAllocationUpdate(
            allocation_id=self._alloc_info.allocation.allocation_id,
            request_state_operation_result=FEAllocationRequestStateOperationResult(
                operation_id=request_state_operation.operation_id,
                status=Status(
                    code=grpc.StatusCode.OK.value[0],
                ),
                prepare_write=FEAllocationRequestStatePrepareWriteOperationResult(),
            ),
        )
        # ID of the write operation, this is not the same as request_state_operation.operation_id.
        write_operation_id: str = request_state_operation.operation_id
        write_operation_info: _RequestStateWriteOperationInfo = (
            _RequestStateWriteOperationInfo(
                operation_id=write_operation_id,
                blob_info=None,
            )
        )
        # Add the info here immediately to prevent duplicate prepares.
        self._pending_request_state_write_operations[write_operation_id] = (
            write_operation_info
        )

        try:
            blob_uri: str = self._request_state_key_blob_uri(
                request_state_operation.state_key
            )
            blob_upload_id: str = await self._blob_store.create_multipart_upload(
                uri=blob_uri,
                logger=self._logger,
            )
            blob_info: _BLOBInfo = _BLOBInfo(
                id=write_operation_id,
                uri=blob_uri,
                upload_id=blob_upload_id,
            )
            write_operation_info.blob_info = blob_info
            blob: FEBLOB = await presign_write_only_blob(
                blob_id=blob_info.id,
                blob_uri=blob_info.uri,
                upload_id=blob_info.upload_id,
                size=request_state_operation.prepare_write.size,
                blob_store=self._blob_store,
                logger=self._logger,
            )
            alloc_update.request_state_operation_result.prepare_write.blob.CopyFrom(
                blob
            )
        except Exception as e:
            self._logger.error(
                "failed to prepare request state write operation",
                exc_info=e,
                operation_id=request_state_operation.operation_id,
            )
            alloc_update.request_state_operation_result.status.code = (
                grpc.StatusCode.INTERNAL.value[0]
            )

        await fe_stub.send_allocation_update(
            alloc_update,
            timeout=_SEND_ALLOCATION_UPDATE_TIMEOUT_SECS,
        )

    async def _reconcile_request_state_write_operation(
        self,
        fe_stub: FunctionExecutorStub,
        request_state_operation: FEAllocationRequestStateOperation,
    ) -> None:
        alloc_update: FEAllocationUpdate = FEAllocationUpdate(
            allocation_id=self._alloc_info.allocation.allocation_id,
            request_state_operation_result=FEAllocationRequestStateOperationResult(
                operation_id=request_state_operation.operation_id,
                status=Status(
                    code=grpc.StatusCode.OK.value[0],
                ),
                commit_write=FEAllocationRequestStateCommitWriteOperationResult(),
            ),
        )

        try:
            write_operation_id: str | None = None
            if request_state_operation.commit_write.HasField("blob"):
                write_operation_id = request_state_operation.commit_write.blob.id

            if write_operation_id in self._pending_request_state_write_operations:
                write_operation_info: _RequestStateWriteOperationInfo = (
                    self._pending_request_state_write_operations.pop(write_operation_id)
                )
                if write_operation_info.blob_info is not None:
                    await self._blob_store.complete_multipart_upload(
                        uri=write_operation_info.blob_info.uri,
                        upload_id=write_operation_info.blob_info.upload_id,
                        parts_etags=[
                            blob_chunk.etag
                            for blob_chunk in request_state_operation.commit_write.blob.chunks
                        ],
                        logger=self._logger,
                    )
            else:
                alloc_update.request_state_operation_result.status.code = (
                    grpc.StatusCode.NOT_FOUND.value[0]
                )
        except Exception as e:
            self._logger.error(
                "failed to commit request state write operation",
                exc_info=e,
                operation_id=request_state_operation.operation_id,
            )
            alloc_update.request_state_operation_result.status.code = (
                grpc.StatusCode.INTERNAL.value[0]
            )

        await fe_stub.send_allocation_update(
            alloc_update,
            timeout=_SEND_ALLOCATION_UPDATE_TIMEOUT_SECS,
        )

    def _request_state_key_blob_uri(
        self,
        state_key: str,
    ) -> str:
        # URL-encode the state key to make it safe for use in S3 URIs.
        # Don't allow any non-url characters and don't allow '/' to prevent
        # S3 directory traversal.
        # i.e. urllib.parse.quote("../../asasdada/foo", safe="") -> '..%2F..%2Fasasdada%2Ffoo'
        # This also allows regular HTTP clients to use the URIs without issues.
        # NB: Max S3 key length is 1024 bytes.
        safe_state_key: str = urllib.parse.quote(state_key, safe="")
        return f"{self._alloc_info.allocation.request_data_payload_uri_prefix}/state/{safe_state_key}"


def _log_alloc_execution_finished(output: AllocationOutput, logger: Any) -> None:
    logger.info(
        "finished running allocation",
        success=output.outcome_code
        == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
        outcome_code=AllocationOutcomeCode.Name(output.outcome_code),
        failure_reason=(
            AllocationFailureReason.Name(output.failure_reason)
            if output.failure_reason is not None
            else None
        ),
    )


def _execution_duration(start_time: float, end_time: float) -> int:
    # <= 0.99 ms functions get billed as 1 ms.
    return math.ceil((end_time - start_time) * 1000)


def _validate_fe_output_blob_request(
    fe_output_blob_request: FEAllocationOutputBLOBRequest,
) -> bool:
    if not fe_output_blob_request.HasField("id"):
        return False

    if not fe_output_blob_request.HasField("size"):
        return False

    # 5 TB is max S3 object size.
    if (
        fe_output_blob_request.size < 0
        or fe_output_blob_request.size > 5 * 1024 * 1024 * 1024 * 1024
    ):
        return False

    return True


def _validate_fe_function_call_watcher(
    fe_function_call_watcher: FEAllocationFunctionCallWatcher,
) -> bool:
    # TODO: Remove watcher_id and function_call_id field support after FE migration period.
    if _proto_field_is_defined(fe_function_call_watcher, "id"):
        return (
            fe_function_call_watcher.HasField("id")
            or fe_function_call_watcher.HasField("watcher_id")
        ) and (
            fe_function_call_watcher.HasField("root_function_call_id")
            or fe_function_call_watcher.HasField("function_call_id")
        )
    else:
        return fe_function_call_watcher.HasField(
            "watcher_id"
        ) and fe_function_call_watcher.HasField("function_call_id")


def _validate_fe_function_call(
    fe_function_call: FEAllocationFunctionCall,
) -> bool:
    try:
        MessageValidator(fe_function_call).optional_blob("args_blob").required_field(
            "updates"
        )
        # TODO: add required id field once all FEs send it.
        # TODO: Validate the update tree.
    except ValueError:
        return False

    updates: FEExecutionPlanUpdates = fe_function_call.updates
    if not updates.HasField("root_function_call_id"):
        return False

    return True


def _to_fe_allocation_outcome_code(
    server_outcome_code: ServerAllocationOutcomeCode,
) -> FEAllocationOutcomeCode:
    if (
        server_outcome_code
        == ServerAllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS
    ):
        return FEAllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS
    elif (
        server_outcome_code
        == ServerAllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE
    ):
        return FEAllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE
    else:
        return FEAllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_UNKNOWN


def _proto_field_is_defined(proto: Any, field_name: str) -> bool:
    """Returns True if the given protobuf field is defined in .proto file."""
    try:
        proto.HasField(field_name)
        return True
    except ValueError:
        return False
