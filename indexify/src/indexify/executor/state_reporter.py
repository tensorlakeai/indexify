import asyncio
import hashlib
import os
import platform
import sys
from dataclasses import dataclass
from socket import gethostname
from typing import Any, Dict, List

from indexify.proto.executor_api_pb2 import (
    AllocationFailureReason,
    AllocationOutcomeCode,
    AllocationResult,
    AllowedFunction,
    ExecutorState,
    ExecutorStatus,
    ExecutorUpdate,
    FunctionCallWatch,
    FunctionExecutorState,
    GPUModel,
    GPUResources,
)
from indexify.proto.executor_api_pb2 import HostResources as HostResourcesProto
from indexify.proto.executor_api_pb2 import (
    ReportExecutorStateRequest,
)
from indexify.proto.executor_api_pb2_grpc import ExecutorAPIStub

from .channel_manager import ChannelManager
from .function_allowlist import FunctionURI
from .function_executor_controller.loggers import allocation_result_logger
from .host_resources.host_resources import HostResources, HostResourcesProvider
from .host_resources.nvidia_gpu import NVIDIA_GPU_MODEL
from .limits import MAX_FUNCTION_CALL_SIZE_MB
from .metrics.state_reporter import (
    metric_state_report_message_fragmentations,
    metric_state_report_message_size_mb,
    metric_state_report_messages_over_size_limit,
    metric_state_report_rpc_errors,
    metric_state_report_rpc_latency,
    metric_state_report_rpcs,
)
from .monitoring.health_checker.health_checker import HealthChecker

# Retry algorithm for state report RPCs is custom due to complexity of state reporting logic.
_MIN_REPORTING_INTERVAL_SEC: float = 5.0
_MAX_REPORTING_INTERVAL_SEC: float = 300.0  # 5 minutes
# The first retry is under 30 sec Executor reporting deadline so Server might not unregister the Executor
# on first retry and we might not lose work already done by allocations running on Executor.
# Subsequent retries back-off exponentially to reduce load on Server and allow Server to unregister the Executor
# to "reset" Server internal state which might mitigate some bugs on Server side.
_REPORTING_BACKOFF_MULTIPLIER: float = (
    3.0  # 15 sec, 45 sec, 135 sec, 300 sec, 300 sec, ...
)
_REPORT_RPC_TIMEOUT_SEC: float = 5.0
# Time to accumulate more state changes before sending another report.
# The larger the value then more latency we add into the system but the less load on Server.
# Large clusters might want to use a larger value to reduce load on Server.
_REPORT_BATCH_DELAY_MS: int = int(
    os.getenv("INDEXIFY_STATE_REPORT_BATCH_DELAY_MS", "0")
)
# Max state report size must be less than max configured Server
# grpc message size (currently 4 GB) minus sufficient headroom to encoding
# and any other overheads.
#
# The max configured grpc message size must be sufficient to send at least
# one allocation result of maximum sizes to ensure progress of the system.
# It also shouldn't be too large to avoid high memory usage on Server side.
#
# We choose 10 MB. This limits memory use on Server side to process state reports
# to reasonable values and prevents excessive fragmentation of state reports
# on Executor side while still allowing to send at least one max sized allocation
# result in a single state report which is 1 MB.
_STATE_REPORT_MAX_MESSAGE_SIZE_MB: float = 10.0

# Add 1 MB for arbitrary message overheads.
if MAX_FUNCTION_CALL_SIZE_MB + 1 > _STATE_REPORT_MAX_MESSAGE_SIZE_MB:
    raise RuntimeError(
        f"MAX_FUNCTION_CALL_SIZE_MB ({MAX_FUNCTION_CALL_SIZE_MB} MB) "
        f"cannot be larger than _STATE_REPORT_MAX_MESSAGE_SIZE_MB "
        f"({_STATE_REPORT_MAX_MESSAGE_SIZE_MB} MB)"
    )


@dataclass
class _FunctionCallWatchInfo:
    watch: FunctionCallWatch
    ref_counter: int


def _function_call_watch_key(watch: FunctionCallWatch) -> str:
    # Allows to group watches for the same function call id into the same group.
    return f"{watch.namespace}.{watch.application}.{watch.request_id}.{watch.function_call_id}"


class ExecutorStateReporter:
    def __init__(
        self,
        executor_id: str,
        version: str,
        labels: Dict[str, str],
        function_allowlist: List[FunctionURI],
        channel_manager: ChannelManager,
        host_resources_provider: HostResourcesProvider,
        health_checker: HealthChecker,
        catalog_entry_name: str | None,
        logger: Any,
    ):
        self._executor_id: str = executor_id
        self._version: str = version
        self._labels: Dict[str, str] = labels.copy()
        self._labels.update(_executor_labels())
        self._hostname: str = gethostname()
        self._channel_manager = channel_manager
        self._health_checker: HealthChecker = health_checker
        self._logger: Any = logger.bind(module=__name__)
        self._allowed_functions: List[AllowedFunction] = _to_allowed_function_protos(
            function_allowlist
        )
        self._catalog_entry_name: str | None = catalog_entry_name
        # We need to fetch total resources only once, because they are not changing.
        self._total_host_resources: HostResources = (
            host_resources_provider.total_host_resources(self._logger)
        )
        self._total_function_executor_resources: HostResources = (
            host_resources_provider.total_function_executor_resources(self._logger)
        )
        self._logger.info(
            "detected host resources",
            total_host_resources=self._total_host_resources,
            total_function_executor_resources=self._total_function_executor_resources,
        )
        self._state_report_worker: asyncio.Task | None = None
        if _REPORT_BATCH_DELAY_MS != 0:
            self._logger.info(
                "state report batching enabled",
                batch_delay_ms=_REPORT_BATCH_DELAY_MS,
            )

        # Mutable fields
        self._state_report_scheduled_event: asyncio.Event = asyncio.Event()
        self._state_reported_event: asyncio.Event = asyncio.Event()
        self._executor_status: ExecutorStatus = ExecutorStatus.EXECUTOR_STATUS_UNKNOWN
        self._last_server_clock: int = (
            0  # Server expects initial value to be 0 until it is set by Server.
        )
        # Alloc ID -> AllocationResult
        self._allocation_results: Dict[str, AllocationResult] = {}
        # FE ID -> FunctionExecutorState
        self._function_executor_states: Dict[str, FunctionExecutorState] = {}
        # Watch content based key -> _FunctionCallWatchInfo
        self._function_call_watches: Dict[str, _FunctionCallWatchInfo] = {}
        self._last_state_report_request: ReportExecutorStateRequest | None = None

    def last_state_report_request(self) -> ReportExecutorStateRequest | None:
        return self._last_state_report_request

    def update_executor_status(self, value: ExecutorStatus) -> None:
        self._executor_status = value

    def update_last_server_clock(self, value: int) -> None:
        self._last_server_clock = value

    def update_function_executor_state(
        self,
        state: FunctionExecutorState,
    ) -> None:
        self._function_executor_states[state.description.id] = state

    def remove_function_executor_state(self, function_executor_id: str) -> None:
        if function_executor_id not in self._function_executor_states:
            self._logger.warning(
                "attempted to remove non-existing function executor state",
                function_executor_id=function_executor_id,
            )
            return

        self._function_executor_states.pop(function_executor_id)

    def add_allocation_result(self, alloc_result: AllocationResult) -> None:
        self._allocation_results[alloc_result.allocation_id] = alloc_result

    def allocation_result_ids(self) -> List[str]:
        """Returns the list of allocation IDs that have unreported allocation results."""
        return list(self._allocation_results.keys())

    def remove_allocation_result(self, alloc_id: str) -> None:
        if alloc_id not in self._allocation_results:
            self._logger.warning(
                "attempted to remove non-existing allocation result",
                allocation_id=alloc_id,
            )
            return

        self._allocation_results.pop(alloc_id)

    def add_function_call_watcher(self, watch: FunctionCallWatch) -> None:
        content_derived_key: str = _function_call_watch_key(watch)
        if content_derived_key not in self._function_call_watches:
            self._function_call_watches[content_derived_key] = _FunctionCallWatchInfo(
                watch=watch,
                ref_counter=0,
            )
        self._function_call_watches[content_derived_key].ref_counter += 1

    def _current_function_call_watches(self) -> List[FunctionCallWatch]:
        return [watch_info.watch for watch_info in self._function_call_watches.values()]

    def remove_function_call_watcher(self, watch: FunctionCallWatch) -> None:
        """Removes a function call watcher.

        If the watcher ref counter reaches zero, the watcher is removed completely.
        If the watcher doesn't exist, a warning is logged.
        """
        content_derived_key: str = _function_call_watch_key(watch)
        watch_info: _FunctionCallWatchInfo | None = self._function_call_watches.get(
            content_derived_key
        )
        if watch_info is None:
            self._logger.warning(
                "attempted to remove non-existing function call watcher",
                watch=str(watch),
            )
            return

        watch_info.ref_counter -= 1
        if watch_info.ref_counter == 0:
            self._function_call_watches.pop(content_derived_key)

    def schedule_state_report(self) -> None:
        """Schedules a state report to be sent to the server asap.

        This method is called when the executor state changes and it needs to get reported.
        The call doesn't block and returns immediately.
        Doesn't raise any exceptions.
        """
        self._state_report_scheduled_event.set()

    async def report_state_and_wait_for_completion(self) -> None:
        """Schedules a state report to be sent to the server asap and waits for the completion of the reporting."""
        self._state_reported_event.clear()
        self.schedule_state_report()
        await self._state_reported_event.wait()

    def run(self) -> None:
        """Runs the state reporter.

        This method is called when the executor starts and it needs to start reporting its state
        periodically.
        """
        self._state_report_worker = asyncio.create_task(
            self._state_report_worker_loop(), name="state_reporter_worker"
        )

    async def shutdown(self) -> None:
        """Tries to do one last state report and shuts down the state reporter.

        Doesn't raise any exceptions."""
        state_report_worker: asyncio.Task = self._state_report_worker
        if state_report_worker is not None:
            # Set to None before cancelling because worker loop checks it to exit.
            self._state_report_worker = None
            state_report_worker.cancel()
            try:
                await state_report_worker
            except asyncio.CancelledError:
                pass  # Expected exception

        # Don't retry state report if it failed during shutdown.
        # We only do best effort last state report and Server might not be available.
        try:
            async with self._channel_manager.create_standalone_channel() as channel:
                await ExecutorAPIStub(channel).report_executor_state(
                    self._collect_report_state_request(),
                    timeout=_REPORT_RPC_TIMEOUT_SEC,
                )
        except Exception as e:
            self._logger.error(
                "failed to report state during shutdown",
                exc_info=e,
            )

    async def _state_report_worker_loop(self) -> None:
        """Runs the state reporter.

        Never raises any exceptions except CancelledError when the executor is shutting down.
        """
        periodic_reporting_interval_sec: float = _MIN_REPORTING_INTERVAL_SEC
        while True:
            stub = ExecutorAPIStub(await self._channel_manager.get_shared_channel())
            while True:
                await self._wait_state_report_scheduled(periodic_reporting_interval_sec)
                # Clear the event because we're reporting the current state now, no need to report the same again.
                self._state_report_scheduled_event.clear()
                report_state_request: ReportExecutorStateRequest = (
                    self._collect_report_state_request()
                )
                try:
                    await self._report_state(stub, report_state_request)
                    self._delete_reported_updates(report_state_request.executor_update)
                    periodic_reporting_interval_sec = _MIN_REPORTING_INTERVAL_SEC
                except Exception as e:
                    periodic_reporting_interval_sec = min(
                        periodic_reporting_interval_sec * _REPORTING_BACKOFF_MULTIPLIER,
                        _MAX_REPORTING_INTERVAL_SEC,
                    )
                    self._logger.error(
                        f"failed to report state to the server, backing-off for {periodic_reporting_interval_sec} sec.",
                        exc_info=e,
                    )

                    # The periodic state reports serve as channel health monitoring requests
                    # (same as TCP keep-alive). Channel Manager returns the same healthy channel
                    # for all RPCs that we do from Executor to Server. So all the RPCs benefit
                    # from this channel health monitoring.
                    self._health_checker.server_connection_state_changed(
                        is_healthy=False,
                        status_message="grpc server channel is unhealthy",
                    )
                    await self._channel_manager.fail_shared_channel()
                    break  # exit the inner loop to use the recreated channel

    async def _wait_state_report_scheduled(
        self, periodic_reporting_interval_sec: float
    ) -> None:
        """Waits until a state report is scheduled.

        Doesn't raise any exceptions except CancelledError when the executor is shutting down.
        """
        wait_state_report_scheduled_event: asyncio.Task = asyncio.create_task(
            self._state_report_scheduled_event.wait(),
            name="wait_state_report_scheduled_event",
        )

        try:
            # Ublock conditions: state report scheduled, timeout waiting or
            # aio cancellation raised as CancelledError with immediate return.
            done, _ = await asyncio.wait(
                [wait_state_report_scheduled_event],
                timeout=periodic_reporting_interval_sec,
            )
        finally:
            wait_state_report_scheduled_event.cancel()

        if len(done) > 0:
            # If state report is not the periodic one then wait a bit more to accumulate more state
            # changes before sending another report to Server.
            await asyncio.sleep(_REPORT_BATCH_DELAY_MS / 1000.0)

    async def _report_state(
        self,
        stub: ExecutorAPIStub,
        request: ReportExecutorStateRequest,
    ) -> None:
        """Report the provided Executor state to Server.

        Raises exception on error.
        """
        _log_reported_state(request, self._logger)
        self._last_state_report_request = request

        with (
            metric_state_report_rpc_errors.count_exceptions(),
            metric_state_report_rpc_latency.time(),
        ):
            metric_state_report_rpcs.inc()
            await stub.report_executor_state(request, timeout=_REPORT_RPC_TIMEOUT_SEC)
        self._state_reported_event.set()
        self._health_checker.server_connection_state_changed(
            is_healthy=True, status_message="grpc server channel is healthy"
        )

    def _collect_report_state_request(self) -> ReportExecutorStateRequest:
        request: ReportExecutorStateRequest = ReportExecutorStateRequest(
            executor_state=self._collect_reported_state(),
            executor_update=ExecutorUpdate(
                executor_id=self._executor_id,
                allocation_results=[],
            ),
        )

        alloc_results: List[AllocationResult] = list(self._allocation_results.values())
        # Always report at least one allocation result to make progress.
        if len(alloc_results) > 0:
            request.executor_update.allocation_results.append(alloc_results.pop())

        request_size: int = request.ByteSize()
        if request_size >= _STATE_REPORT_MAX_MESSAGE_SIZE_MB * 1024 * 1024:
            self._logger.error(
                "state report message size is over the max limit, this might overload server",
                max_message_size_bytes=_STATE_REPORT_MAX_MESSAGE_SIZE_MB * 1024 * 1024,
                message_size_bytes=request_size,
            )
            metric_state_report_messages_over_size_limit.inc()

        while len(alloc_results) != 0:
            alloc_result: AllocationResult = alloc_results.pop()
            request_size += alloc_result.ByteSize()
            if request_size < _STATE_REPORT_MAX_MESSAGE_SIZE_MB * 1024 * 1024:
                request.executor_update.allocation_results.append(alloc_result)
            else:
                self._logger.warning(
                    "state report message size limit reached, fragmenting state report message",
                    max_message_size_bytes=_STATE_REPORT_MAX_MESSAGE_SIZE_MB
                    * 1024
                    * 1024,
                    message_size_bytes=request_size,
                    remaining_allocation_results=len(alloc_results) + 1,
                )
                metric_state_report_message_fragmentations.inc()
                self.schedule_state_report()
                break

        return request

    def _collect_reported_state(self) -> ExecutorState:
        """Returns the current executor state."""
        state = ExecutorState(
            executor_id=self._executor_id,
            hostname=self._hostname,
            version=self._version,
            status=self._executor_status,
            total_function_executor_resources=_to_host_resources_proto(
                self._total_function_executor_resources
            ),
            total_resources=_to_host_resources_proto(self._total_host_resources),
            allowed_functions=self._allowed_functions,
            function_executor_states=list(self._function_executor_states.values()),
            labels=self._labels,
            catalog_entry_name=self._catalog_entry_name,
            function_call_watches=self._current_function_call_watches(),
        )
        state.state_hash = _state_hash(state)
        # Set fields not included in the state hash.
        state.server_clock = self._last_server_clock
        return state

    def _delete_reported_updates(self, update: ExecutorUpdate) -> None:
        for alloc_result in update.allocation_results:
            # Check if the allocation result is removed already by Server from desired state
            # after we reported it.
            if alloc_result.allocation_id in self._allocation_results:
                self.remove_allocation_result(alloc_result.allocation_id)


def _log_reported_state(state: ReportExecutorStateRequest, logger: Any) -> None:
    """Logs the reported executor update.

    Doesn't raise any exceptions."""
    try:
        metric_state_report_message_size_mb.observe(
            state.ByteSize() / (1024.0 * 1024.0)
        )

        for alloc_result in state.executor_update.allocation_results:
            allocation_result_logger(alloc_result, logger).info(
                "reporting allocation outcome",
                outcome_code=AllocationOutcomeCode.Name(alloc_result.outcome_code),
                failure_reason=(
                    AllocationFailureReason.Name(alloc_result.failure_reason)
                    if alloc_result.HasField("failure_reason")
                    else "None"
                ),
                message_size_bytes=alloc_result.ByteSize(),
            )
    except Exception as e:
        logger.error(
            "failed to log reported executor update",
            exc_info=e,
        )


def _to_allowed_function_protos(
    function_allowlist: List[FunctionURI],
) -> List[AllowedFunction]:
    allowed_functions: List[AllowedFunction] = []
    for function_uri in function_allowlist:
        function_uri: FunctionURI
        allowed_function = AllowedFunction(
            namespace=function_uri.namespace,
            application_name=function_uri.application,
            function_name=function_uri.compute_fn,
        )
        if function_uri.version is not None:
            allowed_function.application_version = function_uri.version
        allowed_functions.append(allowed_function)

    return allowed_functions


def _state_hash(state: ExecutorState) -> str:
    serialized_state: bytes = state.SerializeToString(deterministic=True)
    hasher = hashlib.sha256(usedforsecurity=False)
    hasher.update(serialized_state)
    return hasher.hexdigest()


def _to_host_resources_proto(host_resources: HostResources) -> HostResourcesProto:
    proto = HostResourcesProto(
        cpu_count=host_resources.cpu_count,
        memory_bytes=host_resources.memory_mb * 1024 * 1024,
        disk_bytes=host_resources.disk_mb * 1024 * 1024,
    )
    if len(host_resources.gpus) > 0:
        proto.gpu.CopyFrom(
            GPUResources(
                count=len(host_resources.gpus),
                model=_to_gpu_model_proto(
                    host_resources.gpus[0].model
                ),  # All GPUs have the same model
            )
        )
    return proto


def _to_gpu_model_proto(nvidia_gpu_model: NVIDIA_GPU_MODEL) -> GPUModel:
    if nvidia_gpu_model == NVIDIA_GPU_MODEL.A100_40GB:
        return GPUModel.GPU_MODEL_NVIDIA_A100_40GB
    elif nvidia_gpu_model == NVIDIA_GPU_MODEL.A100_80GB:
        return GPUModel.GPU_MODEL_NVIDIA_A100_80GB
    elif nvidia_gpu_model == NVIDIA_GPU_MODEL.H100_80GB:
        return GPUModel.GPU_MODEL_NVIDIA_H100_80GB
    elif nvidia_gpu_model == NVIDIA_GPU_MODEL.TESLA_T4:
        return GPUModel.GPU_MODEL_NVIDIA_TESLA_T4
    elif nvidia_gpu_model == NVIDIA_GPU_MODEL.A6000:
        return GPUModel.GPU_MODEL_NVIDIA_A6000
    elif nvidia_gpu_model == NVIDIA_GPU_MODEL.A10:
        return GPUModel.GPU_MODEL_NVIDIA_A10
    else:
        return GPUModel.GPU_MODEL_UNKNOWN


def _executor_labels() -> Dict[str, str]:
    """Returns standard executor labels always added to user supplied labels."""
    return {
        "os": platform.system(),
        "architecture": platform.machine(),
        "python_major_version": str(sys.version_info.major),
        "python_minor_version": str(sys.version_info.minor),
    }
