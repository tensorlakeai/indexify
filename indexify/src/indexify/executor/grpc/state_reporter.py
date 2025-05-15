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
    FunctionExecutorDescription,
)
from indexify.proto.executor_api_pb2 import (
    FunctionExecutorState as FunctionExecutorStateProto,
)
from indexify.proto.executor_api_pb2 import (
    FunctionExecutorStatus as FunctionExecutorStatusProto,
)
from indexify.proto.executor_api_pb2 import GPUModel as GPUModelProto
from indexify.proto.executor_api_pb2 import GPUResources as GPUResourcesProto
from indexify.proto.executor_api_pb2 import HostResources as HostResourcesProto
from indexify.proto.executor_api_pb2 import (
    ReportExecutorStateRequest,
    ReportTaskOutcomeRequest,
    TaskFailureReason,
    TaskOutcome,
    TaskOutcomeCode,
)
from indexify.proto.executor_api_pb2_grpc import ExecutorAPIStub

from ..function_allowlist import FunctionURI
from ..function_executor.function_executor_state import FunctionExecutorState
from ..function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from ..function_executor.function_executor_status import FunctionExecutorStatus
from ..function_executor.task_output import TaskOutput
from ..host_resources.host_resources import HostResources, HostResourcesProvider
from ..host_resources.nvidia_gpu import NVIDIA_GPU_MODEL
from .channel_manager import ChannelManager
from .metrics.state_reporter import (
    metric_state_report_errors,
    metric_state_report_latency,
    metric_state_report_rpcs,
)

_REPORTING_INTERVAL_SEC = 5
_REPORT_RPC_TIMEOUT_SEC = 5


class ExecutorStateReporter:
    def __init__(
        self,
        executor_id: str,
        version: str,
        labels: Dict[str, str],
        function_allowlist: List[FunctionURI],
        function_executor_states: FunctionExecutorStatesContainer,
        channel_manager: ChannelManager,
        host_resources_provider: HostResourcesProvider,
        logger: Any,
        reporting_interval_sec: int = _REPORTING_INTERVAL_SEC,
    ):
        self._executor_id: str = executor_id
        self._version: str = version
        self._labels: Dict[str, str] = labels.copy()
        self._labels.update(_executor_labels())
        self._hostname: str = gethostname()
        self._function_executor_states: FunctionExecutorStatesContainer = (
            function_executor_states
        )
        self._channel_manager = channel_manager
        self._logger: Any = logger.bind(module=__name__)
        self._reporting_interval_sec: int = reporting_interval_sec
        self._allowed_functions: List[AllowedFunction] = _to_grpc_allowed_functions(
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
        self._completed_task_outputs: List[TaskOutput] = []

    def update_executor_status(self, value: ExecutorStatus) -> None:
        self._executor_status = value

    def update_last_server_clock(self, value: int) -> None:
        self._last_server_clock = value

    def add_completed_task_output(self, task_output: TaskOutput) -> None:
        self._completed_task_outputs.append(task_output)

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

    async def start(self) -> None:
        """Start the state reporter.

        This method is called when the executor starts and it needs to start reporting its state
        periodically. Can be called only once.
        """
        self._state_report_worker = asyncio.create_task(
            self._state_report_worker_loop(), name="state_reporter_worker"
        )
        self._periodic_state_report_scheduler = asyncio.create_task(
            self._periodic_state_report_scheduler_loop(),
            name="state_reporter_periodic_scheduler",
        )

    async def shutdown(self) -> None:
        if self._state_report_worker is not None:
            self._state_report_worker.cancel()
            self._state_report_worker = None
        if self._periodic_state_report_scheduler is not None:
            self._periodic_state_report_scheduler.cancel()
            self._periodic_state_report_scheduler = None

    async def _periodic_state_report_scheduler_loop(self) -> None:
        while True:
            self._state_report_scheduled_event.set()
            await asyncio.sleep(self._reporting_interval_sec)

    async def _state_report_worker_loop(self) -> None:
        """Runs the state reporter.

        Never raises any exceptions.
        """
        while True:
            stub = ExecutorAPIStub(await self._channel_manager.get_channel())
            while True:
                await self._state_report_scheduled_event.wait()
                # Clear the event immidiately to report again asap if needed. This reduces latency in the system.
                self._state_report_scheduled_event.clear()
                try:
                    # The periodic state reports serve as channel health monitoring requests
                    # (same as TCP keep-alive). Channel Manager returns the same healthy channel
                    # for all RPCs that we do from Executor to Server. So all the RPCs benefit
                    # from this channel health monitoring.
                    await self._report_state(stub)
                    self._state_reported_event.set()
                except Exception as e:
                    self._logger.error(
                        f"failed to report state to the server, retrying in {self._reporting_interval_sec} sec.",
                        exc_info=e,
                    )
                    break  # exit the inner loop to recreate the channel if needed

    async def _report_state(self, stub: ExecutorAPIStub):
        """Reports the current state to the server represented by the supplied stub.

        Raises an exception on failure.
        """
        # TODO: Remove this code once all task outcomes are reported via ExecutorState message.
        # Warning: this code needs to go before the state report RPC, because currently Server marks tasks
        # as failed once we report a terminated FE. As a result task outputs are not stored in Server state store.
        # Careful with list modification here as an output can be appended there by another coroutine.
        while len(self._completed_task_outputs) > 0:
            task_output: TaskOutput = self._completed_task_outputs.pop()
            try:
                task_outcome: TaskOutcome = _task_output_to_proto(task_output)
                await _report_task_outcome(
                    task_outcome=task_outcome,
                    stub=stub,
                    executor_id=self._executor_id,
                    logger=self._logger,  # Okay to pass logger which doesn't have the task context because this code is going be delete anyway.
                )
            except Exception as e:
                # We need to re-add the output to the list to retry it later on the next Executor state report.
                self._completed_task_outputs.append(task_output)
                raise

        with (
            metric_state_report_errors.count_exceptions(),
            metric_state_report_latency.time(),
        ):
            metric_state_report_rpcs.inc()
            state = ExecutorState(
                executor_id=self._executor_id,
                hostname=self._hostname,
                version=self._version,
                status=self._executor_status,
                total_function_executor_resources=_host_resources_to_proto(
                    self._total_function_executor_resources
                ),
                total_resources=_host_resources_to_proto(self._total_host_resources),
                allowed_functions=self._allowed_functions,
                function_executor_states=await self._fetch_function_executor_states(),
                labels=self._labels,
            )
            state.state_hash = _state_hash(state)
            # Set fields not included in the state hash.
            state.server_clock = self._last_server_clock

            await stub.report_executor_state(
                ReportExecutorStateRequest(executor_state=state),
                timeout=_REPORT_RPC_TIMEOUT_SEC,
            )

    async def _fetch_function_executor_states(self) -> List[FunctionExecutorStateProto]:
        states = []

        async for function_executor_state in self._function_executor_states:
            function_executor_state: FunctionExecutorState
            function_executor_state_proto = FunctionExecutorStateProto(
                description=FunctionExecutorDescription(
                    id=function_executor_state.id,
                    namespace=function_executor_state.namespace,
                    graph_name=function_executor_state.graph_name,
                    graph_version=function_executor_state.graph_version,
                    function_name=function_executor_state.function_name,
                    secret_names=function_executor_state.secret_names,
                ),
                status=_to_grpc_function_executor_status(
                    function_executor_state.status, self._logger
                ),
            )
            if function_executor_state.image_uri:
                function_executor_state_proto.description.image_uri = (
                    function_executor_state.image_uri
                )
            states.append(function_executor_state_proto)

        return states


def _to_grpc_allowed_functions(function_allowlist: List[FunctionURI]):
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


_STATUS_MAPPING: Dict[FunctionExecutorStatus, Any] = {
    FunctionExecutorStatus.STARTING_UP: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_PENDING,
    FunctionExecutorStatus.STARTUP_FAILED_CUSTOMER_ERROR: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_TERMINATED,
    FunctionExecutorStatus.STARTUP_FAILED_PLATFORM_ERROR: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_TERMINATED,
    FunctionExecutorStatus.IDLE: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_RUNNING,
    FunctionExecutorStatus.RUNNING_TASK: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_RUNNING,
    FunctionExecutorStatus.UNHEALTHY: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_TERMINATED,
    FunctionExecutorStatus.DESTROYING: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_TERMINATED,
    FunctionExecutorStatus.DESTROYED: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_TERMINATED,
    FunctionExecutorStatus.SHUTDOWN: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_TERMINATED,
}


def _to_grpc_function_executor_status(
    status: FunctionExecutorStatus, logger: Any
) -> FunctionExecutorStatusProto:
    result: FunctionExecutorStatusProto = _STATUS_MAPPING.get(
        status, FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_UNKNOWN
    )

    if result == FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_UNKNOWN:
        logger.error("unexpected Function Executor status", status=status)

    return result


def _state_hash(state: ExecutorState) -> str:
    serialized_state: bytes = state.SerializeToString(deterministic=True)
    hasher = hashlib.sha256(usedforsecurity=False)
    hasher.update(serialized_state)
    return hasher.hexdigest()


def _host_resources_to_proto(host_resources: HostResources) -> HostResourcesProto:
    proto = HostResourcesProto(
        cpu_count=host_resources.cpu_count,
        memory_bytes=host_resources.memory_mb * 1024 * 1024,
        disk_bytes=host_resources.disk_mb * 1024 * 1024,
    )
    if len(host_resources.gpus) > 0:
        proto.gpu.CopyFrom(
            GPUResourcesProto(
                count=len(host_resources.gpus),
                model=_gpu_model_to_proto(
                    host_resources.gpus[0].model
                ),  # All GPUs have the same model
            )
        )
    return proto


def _gpu_model_to_proto(gpu_model: NVIDIA_GPU_MODEL) -> GPUModelProto:
    if gpu_model == NVIDIA_GPU_MODEL.A100_40GB:
        return GPUModelProto.GPU_MODEL_NVIDIA_A100_40GB
    elif gpu_model == NVIDIA_GPU_MODEL.A100_80GB:
        return GPUModelProto.GPU_MODEL_NVIDIA_A100_80GB
    elif gpu_model == NVIDIA_GPU_MODEL.H100_80GB:
        return GPUModelProto.GPU_MODEL_NVIDIA_H100_80GB
    elif gpu_model == NVIDIA_GPU_MODEL.TESLA_T4:
        return GPUModelProto.GPU_MODEL_NVIDIA_TESLA_T4
    elif gpu_model == NVIDIA_GPU_MODEL.A6000:
        return GPUModelProto.GPU_MODEL_NVIDIA_A6000
    elif gpu_model == NVIDIA_GPU_MODEL.A10:
        return GPUModelProto.GPU_MODEL_NVIDIA_A10
    else:
        return GPUModelProto.GPU_MODEL_UNKNOWN


def _executor_labels() -> Dict[str, str]:
    """Returns standard executor labels always added to user supplied labels."""
    return {
        "os": platform.system(),
        "architecture": platform.machine(),
        "python_major_version": str(sys.version_info.major),
        "python_minor_version": str(sys.version_info.minor),
    }


def _task_output_to_proto(output: TaskOutput) -> TaskOutcome:
    task_outcome = TaskOutcome(
        task_id=output.task_id,
        namespace=output.namespace,
        graph_name=output.graph_name,
        function_name=output.function_name,
        graph_invocation_id=output.graph_invocation_id,
        reducer=output.reducer,
        outcome_code=output.outcome_code,
        next_functions=(output.router_output.edges if output.router_output else []),
        function_outputs=output.uploaded_data_payloads,
    )
    if output.failure_reason is not None:
        task_outcome.failure_reason = output.failure_reason
    if output.uploaded_stdout is not None:
        task_outcome.stdout.CopyFrom(output.uploaded_stdout)
    if output.uploaded_stderr is not None:
        task_outcome.stderr.CopyFrom(output.uploaded_stderr)

    return task_outcome


async def _report_task_outcome(
    task_outcome: TaskOutcome,
    stub: ExecutorAPIStub,
    executor_id: str,
    logger: Any,
) -> None:
    logger.info(
        "reporting task outcome",
        task_id=task_outcome.task_id,
        namespace=task_outcome.namespace,
        graph_name=task_outcome.graph_name,
        function_name=task_outcome.function_name,
        graph_invocation_id=task_outcome.graph_invocation_id,
        outcome_code=TaskOutcomeCode.Name(task_outcome.outcome_code),
    )

    try:
        await stub.report_task_outcome(
            ReportTaskOutcomeRequest(
                executor_id=executor_id,
                task_outcome=task_outcome,
            ),
            timeout=5.0,
        )
    except Exception as e:
        logger.error("failed to report task outcome", exc_info=e)
        raise
