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
    BLOBChunk as FEBLOBChunk,
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
    AllocationOutcomeCode,
)
from indexify.proto.executor_api_pb2 import (
    FunctionCallRequest as ServerFunctionCallRequest,
)
from indexify.proto.executor_api_pb2 import FunctionCallWatch as ServerFunctionCallWatch
from indexify.proto.executor_api_pb2 import (
    FunctionExecutorTerminationReason,
)
from indexify.proto.executor_api_pb2_grpc import ExecutorAPIStub

from ..blob_store.blob_store import BLOBStore
from ..channel_manager import ChannelManager
from ..state_reconciler import ExecutorStateReconciler
from ..state_reporter import ExecutorStateReporter
from .allocation_info import AllocationInfo
from .allocation_output import AllocationOutput
from .events import AllocationExecutionFinished
from .execution_plan_updates import from_fe_execution_plan_updates
from .metrics.allocation_runner import (
    metric_allocation_runner_allocation_run_latency,
    metric_allocation_runner_allocation_runs,
    metric_allocation_runner_allocation_runs_in_progress,
    metric_function_executor_create_allocation_rpc_errors,
    metric_function_executor_create_allocation_rpc_latency,
)

# The following constants are subject to S3 limits,
# see https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html.
#
# 7 days - max presigned URL validity duration and limit on max function duration
_MAX_PRESIGNED_URI_EXPIRATION_SEC: int = 7 * 24 * 60 * 60
# This chunk size gives the best performance with S3. Based on our benchmarking.
_BLOB_OPTIMAL_CHUNK_SIZE_BYTES: int = 100 * 1024 * 1024  # 100 MB
# Max output size with optimal chunks is 100 * 100 MB = 10 GB.
# Each chunk requires a separate S3 presign operation, so we limit the number of optimal chunks to 100.
# S3 presign operations are local, it typically takes 30 ms per 100 URLs.
_OUTPUT_BLOB_OPTIMAL_CHUNKS_COUNT: int = 100
# This chunk size gives ~20% slower performance with S3 compared to optimal. Based on our benchmarking.
_OUTPUT_BLOB_SLOWER_CHUNK_SIZE_BYTES: int = 1 * 1024 * 1024 * 1024  # 1 GB
# Max output size with slower chunks is 100 * 1 GB = 100 GB.
_OUTPUT_BLOB_SLOWER_CHUNKS_COUNT: int = 100


# FE RPC timeouts
_CREATE_ALLOCATION_TIMEOUT_SECS = 5
_WATCH_ALLOCATION_STATE_TIMEOUT_SECS = 5
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


@dataclass
class _BLOBInfo:
    id: str
    uri: str
    upload_id: str


class AllocationRunner:
    def __init__(
        self,
        alloc_info: AllocationInfo,
        function_executor: FunctionExecutor,
        blob_store: BLOBStore,
        state_reporter: ExecutorStateReporter,
        state_reconciler: ExecutorStateReconciler,
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
        self._state_reconciler: ExecutorStateReconciler = state_reconciler
        self._channel_manager: ChannelManager = channel_manager
        self._logger: Any = logger.bind(module=__name__)
        # BLOB ID -> BLOBInfo
        # Output BLOBs with pending multipart uploads.
        self._pending_output_blobs: Dict[str, _BLOBInfo] = {}
        # Function Call IDs
        self._pending_function_call_ids: Set[str] = set()
        # Function Call Watcher ID -> Function Call Watcher
        self._pending_function_call_watchers: Dict[
            str, FEAllocationFunctionCallWatcher
        ] = {}

    async def run(self) -> AllocationExecutionFinished:
        """Runs the allocation on the Function Executor and sets AllocationInfo.output.

        Doesn't raise any exceptions.
        """
        if self._alloc_info.input is None:
            self._logger.error(
                "allocation input is None, this should never happen",
            )
            self._alloc_info.output = AllocationOutput.internal_error(
                execution_duration_ms=None,
            )
            return AllocationExecutionFinished(
                alloc_info=self._alloc_info,
                function_executor_termination_reason=None,
            )

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
                RequestStateAuthorizationContextManager(
                    function_executor=self._function_executor,
                    allocation_id=self._alloc_info.allocation.allocation_id,
                    request_id=self._alloc_info.allocation.request_id,
                ),
            ):
                metric_allocation_runner_allocation_runs.inc()
                fe_alloc_result: FEAllocationResult = await self._run_allocation_on_fe()
                return AllocationOutput.from_allocation_result(
                    fe_result=fe_alloc_result,
                    execution_duration_ms=_execution_duration(
                        execution_start_time, time.monotonic()
                    ),
                    logger=self._logger,
                )
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
                self._alloc_info.output = AllocationOutput.function_executor_unresponsive_after_running_allocation(
                    execution_duration_ms=_execution_duration(
                        execution_start_time, time.monotonic()
                    ),
                )
                self._logger.info(
                    "Function Executor is unresponsive after running the allocation, shutting down Function Executor"
                )
        except BaseException as e:
            # This is an unexpected exception in our Executor code; we believe that this indicates an internal error.
            # We still charge the user to prevent service abuse scenarios (like a DDOS atack on Executor).
            self._logger.error(
                "unexpected error during allocation execution",
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

    async def _run_allocation_on_fe(self) -> FEAllocationResult:
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

        channel: grpc.aio.Channel = self._function_executor.channel()
        fe_stub = FunctionExecutorStub(channel)

        try:
            with (
                metric_function_executor_create_allocation_rpc_errors.count_exceptions(),
                metric_function_executor_create_allocation_rpc_latency.time(),
            ):
                await fe_stub.create_allocation(
                    FECreateAllocationRequest(allocation=fe_alloc),
                    timeout=_CREATE_ALLOCATION_TIMEOUT_SECS,
                )
        except grpc.aio.AioRpcError as e:
            _raise_from_grpc_error(e)

        try:
            watch_allocation_state_rpc = fe_stub.watch_allocation_state(
                FEWatchAllocationStateRequest(
                    allocation_id=self._alloc_info.allocation.allocation_id
                ),
                timeout=_WATCH_ALLOCATION_STATE_TIMEOUT_SECS,
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
                    watch_allocation_state_rpc.read(),
                    timeout=deadline - time.monotonic(),
                )

                if response == grpc.aio.EOF:
                    break

                if response.HasField("result"):
                    allocation_result = response.result
                    break

                if response.HasField("progress"):
                    if previous_progress != response.progress:
                        deadline = time.monotonic() + no_progress_timeout_sec
                    previous_progress = response.progress

                # Reconcile output BLOBs.
                fe_output_blob_requests: Dict[str, FEAllocationOutputBLOBRequest] = {}
                for output_blob_request in response.output_blob_requests:
                    if not _validate_fe_output_blob_request(output_blob_request):
                        self._logger.warning(
                            "skipping invalid FE output BLOB request",
                            output_blob_request=str(output_blob_request),
                        )
                        continue
                    fe_output_blob_requests[output_blob_request.id] = (
                        output_blob_request
                    )

                    if output_blob_request.id not in self._pending_output_blobs:
                        # TODO: make this non-blocking to not block the function execution.
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

                for output_blob_id in list(self._pending_output_blobs.keys()):
                    if output_blob_id not in fe_output_blob_requests:
                        del self._pending_output_blobs[output_blob_id]

                # Reconcile function calls.
                fe_function_calls: Dict[str, FEAllocationFunctionCall] = {}
                for function_call in response.function_calls:
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
                        not in self._pending_function_call_ids
                    ):
                        # TODO: make this non-blocking to not block the function execution.
                        await self._create_function_call(function_call)

                for function_call_id in list(self._pending_function_call_ids):
                    if function_call_id not in fe_function_calls:
                        self._pending_function_call_ids.remove(function_call_id)

                # Reconcile function call watchers.
                fe_function_call_watchers: Dict[
                    str, FEAllocationFunctionCallWatcher
                ] = {}
                for function_call_watcher in response.function_call_watchers:
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
                            in self._pending_function_call_ids
                        ):
                            # TODO: make this non-blocking to not block the function execution.
                            await self._create_function_call_watcher(
                                function_call_watcher
                            )
                        else:
                            # Send "Not found" to FE because function call doesn't exist.
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
                            self._pending_function_call_watchers[
                                function_call_watcher_id
                            ]
                        )
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
            watch_allocation_state_rpc.cancel()

        try:
            await fe_stub.delete_allocation(
                FEDeleteAllocationRequest(
                    allocation_id=self._alloc_info.allocation.allocation_id
                ),
                timeout=_DELETE_ALLOCATION_TIMEOUT_SECS,
            )
        except grpc.aio.AioRpcError as e:
            _raise_from_grpc_error(e)

        return allocation_result

    async def _create_output_blob(
        self, fe_output_blob_request: FEAllocationOutputBLOBRequest
    ) -> FEBLOB:
        function_outputs_blob_uri: str = (
            f"{self._alloc_info.allocation.output_payload_uri_prefix}.{self._alloc_info.allocation.allocation_id}.{fe_output_blob_request.id}.output"
        )
        function_outputs_blob_upload_id = (
            await self._blob_store.create_multipart_upload(
                uri=function_outputs_blob_uri,
                logger=self._logger,
            )
        )
        blob_info: _BLOBInfo = _BLOBInfo(
            id=fe_output_blob_request.id,
            uri=function_outputs_blob_uri,
            upload_id=function_outputs_blob_upload_id,
        )

        try:
            function_outputs_blob: FEBLOB = (
                await _presign_function_outputs_blob(
                    uri=function_outputs_blob_uri,
                    upload_id=function_outputs_blob_upload_id,
                    blob_store=self._blob_store,
                    logger=self._logger,
                ),
            )
            self._pending_output_blobs[fe_output_blob_request.id] = blob_info
            return function_outputs_blob
        except BaseException:
            await self._blob_store.abort_multipart_upload(
                uri=function_outputs_blob_uri,
                upload_id=function_outputs_blob_upload_id,
                logger=self._logger,
            )
            raise

    async def _create_function_call(
        self, fe_function_call: FEAllocationFunctionCall
    ) -> None:
        """Creates a function call in the server.

        If the creation fails then doesn't do anything."""
        runs_left = _SERVER_CALL_FUNCTION_RPC_MAX_RETRIES + 1
        while True:
            try:
                await ExecutorAPIStub(
                    self._channel_manager.get_shared_channel()
                ).call_function(
                    ServerFunctionCallRequest(
                        namespace=self._alloc_info.allocation.function.namespace,
                        application=self._alloc_info.allocation.function.application_name,
                        request_id=self._alloc_info.allocation.request_id,
                        source_function_call_id=self._alloc_info.allocation.function_call_id,
                        updates=from_fe_execution_plan_updates(
                            fe_function_call.updates
                        ),
                    ),
                    timeout=_SERVER_CALL_FUNCTION_RPC_TIMEOUT_SECS,
                )
                break
            except grpc.aio.AioRpcError as e:
                runs_left -= 1
                if runs_left == 0:
                    # It's important to not do anything here. If user creates a function call watcher then they'll
                    # get error because function call wasn't created.
                    return
                else:
                    self._logger.error(
                        f"call_function RPC failed, retrying in {_SERVER_CALL_FUNCTION_RPC_BACKOFF_SECS} seconds",
                        runs_left=runs_left,
                        exc_info=e,
                    )
                    await asyncio.sleep(_SERVER_CALL_FUNCTION_RPC_BACKOFF_SECS)

        self._pending_function_call_ids.add(
            fe_function_call.updates.root_function_call_id
        )

    async def _create_function_call_watcher(
        self, fe_function_call_watcher: FEAllocationFunctionCallWatcher
    ) -> None:
        """Creates a function call watcher in the server.

        If the creation fails then doesn't do anything."""
        # Don't rely on FE provided ID, could be an attack vector.
        executor_watcher_id: str = "{}.{}.{}".format(
            self._alloc_info.allocation.function.namespace,
            self._alloc_info.allocation.request_id,
            fe_function_call_watcher.watcher_id,
        )
        # TODO: Add a "run once" callback for the watched function call completion to state reconciler.
        # self._state_reconciler.add_
        self._state_reporter.add_function_call_watcher(
            watcher_id=executor_watcher_id,
            watcher=ServerFunctionCallWatch(
                namespace=self._alloc_info.allocation.function.namespace,
                application=self._alloc_info.allocation.function.application_name,
                request_id=self._alloc_info.allocation.request_id,
                function_call_id=fe_function_call_watcher.function_call_id,
            ),
        )
        self._pending_function_call_watchers[fe_function_call_watcher.watcher_id] = (
            fe_function_call_watcher
        )

    async def _delete_function_call_watcher(
        self, fe_function_call_watcher: FEAllocationFunctionCallWatcher
    ) -> None:
        """Deletes a function call watcher in the server.

        If the deletion fails then doesn't do anything."""
        executor_watcher_id: str = "{}.{}.{}".format(
            self._alloc_info.allocation.function.namespace,
            self._alloc_info.allocation.request_id,
            fe_function_call_watcher.watcher_id,
        )
        self._state_reporter.remove_function_call_watcher(executor_watcher_id)
        del self._pending_function_call_watchers[fe_function_call_watcher.watcher_id]
        self._pending_function_call_ids.remove(
            fe_function_call_watcher.function_call_id
        )


async def _presign_function_outputs_blob(
    blob_info: _BLOBInfo, blob_store: BLOBStore, logger: Any
) -> FEBLOB:
    """Presigns the output blob for the allocation."""
    chunks: list[FEBLOBChunk] = []

    while len(chunks) != (
        _OUTPUT_BLOB_OPTIMAL_CHUNKS_COUNT + _OUTPUT_BLOB_SLOWER_CHUNKS_COUNT
    ):
        upload_chunk_uri: str = await blob_store.presign_upload_part_uri(
            uri=blob_info.uri,
            part_number=len(chunks) + 1,
            upload_id=blob_info.upload_id,
            expires_in_sec=_MAX_PRESIGNED_URI_EXPIRATION_SEC,
            logger=logger,
        )

        chunk_size: int = (
            _BLOB_OPTIMAL_CHUNK_SIZE_BYTES
            if len(chunks) < _OUTPUT_BLOB_OPTIMAL_CHUNKS_COUNT
            else _OUTPUT_BLOB_SLOWER_CHUNK_SIZE_BYTES
        )
        chunks.append(
            FEBLOBChunk(
                uri=upload_chunk_uri,
                size=chunk_size,
                # ETag is only set by FE when returning BLOBs to us
            )
        )

    return FEBLOB(
        id=blob_info.id,
        chunks=chunks,
    )


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


def _raise_from_grpc_error(e: grpc.aio.AioRpcError) -> None:
    if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        raise _AllocationTimeoutError() from e
    else:
        raise _AllocationFailedLeavingFEInUndefinedState() from e


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
        MessageValidator(fe_function_call).required_blob("args_blob").required_field(
            "updates"
        )
    except ValueError:
        return False

    updates: FEExecutionPlanUpdates = fe_function_call.updates
    if not updates.HasField("root_function_call_id"):
        return False

    return True
