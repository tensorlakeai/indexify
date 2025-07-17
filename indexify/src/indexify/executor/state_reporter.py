import asyncio
import hashlib
import platform
import sys
from socket import gethostname
from typing import Any, Dict, List, Optional

from indexify.proto.executor_api_pb2 import (
    AllowedFunction,
    ExecutorState,
    ExecutorStatus,
    ExecutorUpdate,
    FunctionExecutorState,
    FunctionExecutorUpdate,
    GPUModel,
    GPUResources,
)
from indexify.proto.executor_api_pb2 import HostResources as HostResourcesProto
from indexify.proto.executor_api_pb2 import (
    ReportExecutorStateRequest,
    TaskFailureReason,
    TaskOutcomeCode,
    TaskResult,
)
from indexify.proto.executor_api_pb2_grpc import ExecutorAPIStub

from .channel_manager import ChannelManager
from .function_allowlist import FunctionURI
from .function_executor_controller.loggers import task_result_logger
from .host_resources.host_resources import HostResources, HostResourcesProvider
from .host_resources.nvidia_gpu import NVIDIA_GPU_MODEL
from .metrics.state_reporter import (
    metric_state_report_rpc_errors,
    metric_state_report_rpc_latency,
    metric_state_report_rpcs,
)
from .monitoring.health_checker.health_checker import HealthChecker

_REPORTING_INTERVAL_SEC = 5
_REPORTING_BACKOFF_SEC = 5
_REPORT_RPC_TIMEOUT_SEC = 5


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
        self._state_report_worker: Optional[asyncio.Task] = None
        self._periodic_state_report_scheduler: Optional[asyncio.Task] = None

        # Mutable fields
        self._state_report_scheduled_event: asyncio.Event = asyncio.Event()
        self._state_reported_event: asyncio.Event = asyncio.Event()
        self._executor_status: ExecutorStatus = ExecutorStatus.EXECUTOR_STATUS_UNKNOWN
        self._last_server_clock: int = (
            0  # Server expects initial value to be 0 until it is set by Server.
        )
        self._pending_task_results: List[TaskResult] = []
        self._pending_fe_updates: List[FunctionExecutorUpdate] = []
        self._function_executor_states: Dict[str, FunctionExecutorState] = {}

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

    def add_completed_task_result(self, task_result: TaskResult) -> None:
        self._pending_task_results.append(task_result)

    def add_function_executor_update(self, update: FunctionExecutorUpdate) -> None:
        """Adds a function executor update to the list of updates to be reported."""
        self._pending_fe_updates.append(update)

    def schedule_state_report(self) -> None:
        """Schedules a state report to be sent to the server asap.

        This method is called when the executor state changes and it needs to get reported.
        The call doesn't block and returns immediately.
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
        self._periodic_state_report_scheduler = asyncio.create_task(
            self._periodic_state_report_scheduler_loop(),
            name="state_reporter_periodic_scheduler",
        )

    async def shutdown(self) -> None:
        """Tries to do one last state report and shuts down the state reporter.

        Doesn't raise any exceptions."""
        if self._state_report_worker is not None:
            self._state_report_worker.cancel()
            try:
                await self._state_report_worker
            except asyncio.CancelledError:
                pass  # Expected exception
            self._state_report_worker = None

        if self._periodic_state_report_scheduler is not None:
            self._periodic_state_report_scheduler.cancel()
            try:
                await self._periodic_state_report_scheduler
            except asyncio.CancelledError:
                pass
            self._periodic_state_report_scheduler = None

        # Don't retry state report if it failed during shutdown.
        # We only do best effort last state report and Server might not be available.
        try:
            async with self._channel_manager.create_standalone_channel() as channel:
                await ExecutorAPIStub(channel).report_executor_state(
                    ReportExecutorStateRequest(
                        executor_state=self._current_executor_state(),
                        executor_update=self._remove_pending_update(),
                    ),
                    timeout=_REPORT_RPC_TIMEOUT_SEC,
                )
        except Exception as e:
            self._logger.error(
                "failed to report state during shutdown",
                exc_info=e,
            )

    async def _periodic_state_report_scheduler_loop(self) -> None:
        while True:
            self._state_report_scheduled_event.set()
            await asyncio.sleep(_REPORTING_INTERVAL_SEC)

    async def _state_report_worker_loop(self) -> None:
        """Runs the state reporter.

        Never raises any exceptions.
        """
        while True:
            stub = ExecutorAPIStub(await self._channel_manager.get_shared_channel())
            while True:
                await self._state_report_scheduled_event.wait()
                # Clear the event immidiately to report again asap if needed. This reduces latency in the system.
                self._state_report_scheduled_event.clear()
                try:
                    state: ExecutorState = self._current_executor_state()
                    update: ExecutorUpdate = self._remove_pending_update()
                    _log_reported_executor_update(update, self._logger)

                    with (
                        metric_state_report_rpc_errors.count_exceptions(),
                        metric_state_report_rpc_latency.time(),
                    ):
                        metric_state_report_rpcs.inc()
                        await stub.report_executor_state(
                            ReportExecutorStateRequest(
                                executor_state=state, executor_update=update
                            ),
                            timeout=_REPORT_RPC_TIMEOUT_SEC,
                        )
                    self._state_reported_event.set()
                    self._health_checker.server_connection_state_changed(
                        is_healthy=True, status_message="grpc server channel is healthy"
                    )
                except Exception as e:
                    self._add_to_pending_update(update)
                    self._logger.error(
                        f"failed to report state to the server, backing-off for {_REPORTING_BACKOFF_SEC} sec.",
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
                    await asyncio.sleep(_REPORTING_BACKOFF_SEC)
                    break  # exit the inner loop to use the recreated channel

    def _current_executor_state(self) -> ExecutorState:
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
        )
        state.state_hash = _state_hash(state)
        # Set fields not included in the state hash.
        state.server_clock = self._last_server_clock
        return state

    def _remove_pending_update(self) -> ExecutorUpdate:
        """Removes all pending executor updates and returns them."""
        # No races here cause we don't await.
        task_results: List[TaskResult] = self._pending_task_results
        self._pending_task_results = []

        fe_updates: List[FunctionExecutorUpdate] = self._pending_fe_updates
        self._pending_fe_updates = []

        return ExecutorUpdate(
            executor_id=self._executor_id,
            task_results=task_results,
            function_executor_updates=fe_updates,
        )

    def _add_to_pending_update(self, update: ExecutorUpdate) -> None:
        for task_result in update.task_results:
            self.add_completed_task_result(task_result)
        for function_executor_update in update.function_executor_updates:
            self.add_function_executor_update(function_executor_update)


def _log_reported_executor_update(update: ExecutorUpdate, logger: Any) -> None:
    """Logs the reported executor update.

    Doesn't raise any exceptions."""
    try:
        for task_result in update.task_results:
            task_result_logger(task_result, logger).info(
                "reporting task outcome",
                outcome_code=TaskOutcomeCode.Name(task_result.outcome_code),
                failure_reason=(
                    TaskFailureReason.Name(task_result.failure_reason)
                    if task_result.HasField("failure_reason")
                    else "None"
                ),
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
            graph_name=function_uri.compute_graph,
            function_name=function_uri.compute_fn,
        )
        if function_uri.version is not None:
            allowed_function.graph_version = function_uri.version
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
