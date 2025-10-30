import asyncio
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, Set

import grpc
from tensorlake.function_executor.proto.function_executor_pb2 import BLOB as FEBLOB
from tensorlake.function_executor.proto.function_executor_pb2 import (
    Allocation as FEAllocation,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationFunctionCall as FEAllocationFunctionCall,
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
    AllocationOutputBLOBRequest as FEAllocationOutputBLOBRequest,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationProgress as FEAllocationProgress,
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

from indexify.executor.function_executor.function_executor import FunctionExecutor
from indexify.executor.function_executor.health_checker import HealthCheckResult
from indexify.executor.function_executor.server.function_executor_server import (
    FunctionExecutorServerStatus,
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

from ..blob_store.blob_store import BLOBStore
from ..channel_manager import ChannelManager
from ..state_reporter import ExecutorStateReporter
from .aio_utils import shielded_await
from .allocation_info import AllocationInfo
from .allocation_output import AllocationOutput
from .blob_utils import (
    data_payload_to_serialized_object_inside_blob,
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
)

# FE RPC timeouts
_CREATE_ALLOCATION_TIMEOUT_SECS = 5
_SEND_ALLOCATION_UPDATE_TIMEOUT_SECS = 5
_DELETE_ALLOCATION_TIMEOUT_SECS = 5


# Server RPC settings
_SERVER_CALL_FUNCTION_RPC_TIMEOUT_SECS = 5
_SERVER_CALL_FUNCTION_RPC_BACKOFF_SECS = 2
_SERVER_CALL_FUNCTION_RPC_MAX_RETRIES = 3


class _AllocationTimeoutError(Exception):
    """Raised when allocation execution times out.

    Used internally in AllocationRunner.
    """


class _AllocationFailedLeavingFEInUndefinedState(Exception):
    """Raised when allocation execution fails leaving FE in undefined state.

    Used internally in AllocationRunner.
    """


class _AllocationFailedDueToUserError(Exception):
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
        # BLOB ID -> BLOBInfo
        # Output BLOBs with pending multipart uploads.
        # BLOB is deleted once its upload is either completed or aborted.
        self._pending_output_blobs: Dict[str, _BLOBInfo] = {}
        # Function Call IDs ever started in this allocation.
        # This is an append-only set.
        self._started_function_call_ids: Set[str] = set()
        # FE Function Call Watcher ID -> _FunctionCallWatcherInfo
        self._pending_function_call_watchers: Dict[str, _FunctionCallWatcherInfo] = {}

    async def destroy(self) -> None:
        """Releases all the resources held by the AllocationRunner.

        Doesn't raise any exceptions. Idempotent.
        """
        while len(self._pending_output_blobs) > 0:
            try:
                output_blob: _BLOBInfo = self._pending_output_blobs.popitem()[1]
                await self._blob_store.abort_multipart_upload(
                    uri=output_blob.uri,
                    upload_id=output_blob.upload_id,
                    logger=self._logger,
                )
            except BaseException as e:
                # Also catches any aio cancellations because it's important to clean up all resources.
                self._logger.error(
                    "failed to abort pending output blob multipart upload during AllocationRunner destruction",
                    blob_id=output_blob.id,
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
                RequestStateAuthorizationContextManager(
                    function_executor=self._function_executor,
                    allocation_id=self._alloc_info.allocation.allocation_id,
                    request_id=self._alloc_info.allocation.request_id,
                ),
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
        # Timeout in seconds for detecting no progress in allocation execution.
        no_progress_timeout_sec: float = self._alloc_info.allocation_timeout_ms / 1000.0
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

        # The deadline is extended only on each progress message.
        deadline: float = time.monotonic() + no_progress_timeout_sec
        try:
            while True:
                response: FEAllocationState | grpc.aio.EOF = await asyncio.wait_for(
                    watch_allocation_state_rpc_stream.read(),
                    timeout=deadline - time.monotonic(),
                )

                if response == grpc.aio.EOF:
                    break

                if response.HasField("progress"):
                    if previous_progress != response.progress:
                        deadline = time.monotonic() + no_progress_timeout_sec
                    previous_progress = response.progress

                # TODO: make reconciliations non-blocking to not block the function execution.
                await self._reconcile_output_blobs(
                    fe_stub, response.output_blob_requests
                )
                await self._reconcile_function_calls(response.function_calls)
                await self._reconcile_function_call_watchers(
                    fe_stub, response.function_call_watchers
                )

                # Check the result last to make sure we reconciled the last state.
                if response.HasField("result"):
                    allocation_result = response.result
                    break
        except asyncio.TimeoutError:
            # This is asyncio.wait_for(watch_allocation_state_rpc.read()) timeout.
            raise _AllocationTimeoutError()
        except grpc.aio.AioRpcError as e:
            # This is regular gRPC call error.
            _raise_from_grpc_error(e)
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
                    # As this is FE error it's okay to mark it as unhealthy.
                    raise _AllocationFailedDueToUserError()

        return _RunAllocationOnFunctionExecutorResult(
            fe_result=allocation_result,
            execution_end_time=execution_end_time,
            function_outputs_blob_uri=function_outputs_blob_uri,
        )

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
                output_blob: FEBLOB = await self._create_output_blob(
                    output_blob_request
                )
                await fe_stub.send_allocation_update(
                    FEAllocationUpdate(
                        allocation_id=self._alloc_info.allocation.allocation_id,
                        output_blob=output_blob,
                    ),
                    timeout=_SEND_ALLOCATION_UPDATE_TIMEOUT_SECS,
                )

        # When user deletes blob request we should not delete the blob because
        # blob is separate from its request. The blob will be deleted when we
        # finalize its upload.

    async def _create_output_blob(
        self, fe_output_blob_request: FEAllocationOutputBLOBRequest
    ) -> FEBLOB:
        blob_uri: str = (
            f"{self._alloc_info.allocation.output_payload_uri_prefix}.{self._alloc_info.allocation.allocation_id}.{fe_output_blob_request.id}.output"
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
        function_calls: list[FEAllocationFunctionCall],
    ) -> None:
        fe_function_calls: Dict[str, FEAllocationFunctionCall] = {}
        for function_call in function_calls:
            if not _validate_fe_function_call(function_call):
                self._logger.warning(
                    "skipping invalid FE function call",
                    function_call=str(function_call),
                )
                continue

            fe_function_calls[function_call.updates.root_function_call_id] = (
                function_call
            )

            if (
                function_call.updates.root_function_call_id
                not in self._started_function_call_ids
            ):
                await self._create_function_call(function_call)

        # Don't delete function calls because _started_function_call_ids is append only.

    async def _create_function_call(
        self, fe_function_call: FEAllocationFunctionCall
    ) -> None:
        """Creates a function call in the server.

        If the creation fails then doesn't do anything."""
        # It's important to not do anything in this function if user creates a function
        # call watcher then they'll get error because function call wasn't created.
        #
        # TODO: Add a message to FE updates which allows to report function call creation failure back to FE.
        # The FE can raise exception in function call creation call stack so user code can see the exception.

        blob_info: _BLOBInfo | None = None
        if fe_function_call.HasField("args_blob"):
            blob_info = self._pending_output_blobs.get(fe_function_call.args_blob.id)
            if blob_info is None:
                # Either args blob upload is already completed or FE sent an invalid blob ID.
                self._logger.info(
                    "skipping function call creation because args blob is not found on Executor side",
                    child_fn_call_id=fe_function_call.updates.root_function_call_id,
                    args_blob_id=fe_function_call.args_blob.id,
                )
                return

        try:
            if blob_info is not None:
                await self._complete_blob_upload(blob_info, fe_function_call.args_blob)
        except Exception as e:
            self._logger.error(
                "failed to complete args blob upload for function call creation",
                child_fn_call_id=fe_function_call.updates.root_function_call_id,
                args_blob_id=fe_function_call.args_blob.id,
                exc_info=e,
            )
            return

        try:
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
        except ValueError as e:
            self._logger.info(
                "skipping child function call creation due to invalid execution plan updates",
                exc_info=e,
                child_fn_call_id=fe_function_call.updates.root_function_call_id,
            )
            return

        # Do Retries because network partitions from Executor to Server might be taking place.
        runs_left: int = _SERVER_CALL_FUNCTION_RPC_MAX_RETRIES + 1
        while True:
            try:
                ExecutorAPIStub(
                    await self._channel_manager.get_shared_channel()
                ).call_function(
                    server_function_call_request,
                    timeout=_SERVER_CALL_FUNCTION_RPC_TIMEOUT_SECS,
                )
                break
            except grpc.aio.AioRpcError as e:
                runs_left -= 1
                if runs_left == 0:
                    return  # Do nothing, TODO: report failure to FE.
                else:
                    self._logger.error(
                        f"call_function RPC failed, retrying in {_SERVER_CALL_FUNCTION_RPC_BACKOFF_SECS} seconds",
                        runs_left=runs_left,
                        exc_info=e,
                    )
                    await asyncio.sleep(_SERVER_CALL_FUNCTION_RPC_BACKOFF_SECS)

        self._started_function_call_ids.add(
            fe_function_call.updates.root_function_call_id
        )

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

            fe_function_call_watchers[function_call_watcher.watcher_id] = (
                function_call_watcher
            )

            if (
                function_call_watcher.watcher_id
                not in self._pending_function_call_watchers
            ):
                if (
                    function_call_watcher.function_call_id
                    in self._started_function_call_ids
                ):
                    await self._create_function_call_watcher(function_call_watcher)
                else:
                    # Send "Not found" to FE because function call doesn't exist.
                    self._logger.info(
                        "reporting function call as failed to FE because it is not found",
                        child_fn_call_id=function_call_watcher.function_call_id,
                    )
                    await fe_stub.send_allocation_update(
                        FEAllocationUpdate(
                            allocation_id=self._alloc_info.allocation.allocation_id,
                            function_call_result=FEAllocationFunctionCallResult(
                                function_call_id=function_call_watcher.function_call_id,
                                outcome_code=FEAllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                            ),
                        ),
                        timeout=_SEND_ALLOCATION_UPDATE_TIMEOUT_SECS,
                    )

        for function_call_watcher_id in list(
            self._pending_function_call_watchers.keys()
        ):
            if function_call_watcher_id not in fe_function_call_watchers:
                await self._delete_function_call_watcher(
                    self._pending_function_call_watchers[function_call_watcher_id]
                )

    async def _create_function_call_watcher(
        self, fe_function_call_watcher: FEAllocationFunctionCallWatcher
    ) -> None:
        """Creates a function call watcher in the server.

        If the creation fails then doesn't do anything."""
        watcher_info: _FunctionCallWatcherInfo = _FunctionCallWatcherInfo(
            fe_watcher_id=fe_function_call_watcher.watcher_id,
            server_watch=ServerFunctionCallWatch(
                namespace=self._alloc_info.allocation.function.namespace,
                application=self._alloc_info.allocation.function.application_name,
                request_id=self._alloc_info.allocation.request_id,
                function_call_id=fe_function_call_watcher.function_call_id,
            ),
            task=None,
            result_queue=asyncio.Queue(maxsize=1),
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


class RequestStateAuthorizationContextManager:
    def __init__(
        self, function_executor: FunctionExecutor, allocation_id: str, request_id: str
    ):
        self._function_executor: FunctionExecutor = function_executor
        self._allocation_id: str = allocation_id
        self._request_id: str = request_id

    def __enter__(self):
        self._function_executor.request_state_client().add_allocation_to_request_id_entry(
            allocation_id=self._allocation_id,
            request_id=self._request_id,
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._function_executor.request_state_client().remove_allocation_to_request_id_entry(
            allocation_id=self._allocation_id,
        )


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
    if not fe_function_call_watcher.HasField("watcher_id"):
        return False

    if not fe_function_call_watcher.HasField("function_call_id"):
        return False

    return True


def _validate_fe_function_call(
    fe_function_call: FEAllocationFunctionCall,
) -> bool:
    try:
        MessageValidator(fe_function_call).optional_blob("args_blob").required_field(
            "updates"
        )
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
