import asyncio
import hashlib
from socket import gethostname
from typing import Any, Dict, List, Optional

from indexify.proto.executor_api_pb2 import (
    AllowedFunction,
)
from indexify.proto.executor_api_pb2 import ExecutorFlavor as ExecutorFlavorProto
from indexify.proto.executor_api_pb2 import (
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
)
from indexify.proto.executor_api_pb2_grpc import ExecutorAPIStub

from ..api_objects import FunctionURI
from ..executor_flavor import ExecutorFlavor
from ..function_executor.function_executor_state import FunctionExecutorState
from ..function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from ..function_executor.function_executor_status import FunctionExecutorStatus
from ..host_resources.host_resources import HostResources, HostResourcesProvider
from ..host_resources.nvidia_gpu import NVIDIA_GPU_MODEL
from ..runtime_probes import RuntimeProbes
from .channel_manager import ChannelManager
from .metrics.state_reporter import (
    metric_state_report_errors,
    metric_state_report_latency,
    metric_state_report_rpcs,
)

_REPORTING_INTERVAL_SEC = 5
_REPORT_RPC_TIMEOUT_SEC = 5
_REPORT_BACKOFF_ON_ERROR_SEC = 5


class ExecutorStateReporter:
    def __init__(
        self,
        executor_id: str,
        flavor: ExecutorFlavor,
        version: str,
        labels: Dict[str, str],
        development_mode: bool,
        function_allowlist: Optional[List[FunctionURI]],
        function_executor_states: FunctionExecutorStatesContainer,
        channel_manager: ChannelManager,
        host_resources_provider: HostResourcesProvider,
        logger: Any,
        reporting_interval_sec: int = _REPORTING_INTERVAL_SEC,
    ):
        self._executor_id: str = executor_id
        self._flavor: ExecutorFlavor = flavor
        self._version: str = version
        self._labels: Dict[str, str] = labels.copy()
        self._development_mode: bool = development_mode
        self._hostname: str = gethostname()
        self._function_executor_states: FunctionExecutorStatesContainer = (
            function_executor_states
        )
        self._channel_manager = channel_manager
        self._host_resources_provider: HostResourcesProvider = host_resources_provider
        self._logger: Any = logger.bind(module=__name__)
        self._reporting_interval_sec: int = reporting_interval_sec
        self._total_host_resources: Optional[HostResourcesProto] = None
        self._total_function_executor_resources: Optional[HostResourcesProto] = None

        self._is_shutdown: bool = False
        self._executor_status: ExecutorStatus = ExecutorStatus.EXECUTOR_STATUS_UNKNOWN
        self._allowed_functions: List[AllowedFunction] = _to_grpc_allowed_functions(
            function_allowlist
        )
        self._labels.update(_label_values_to_strings(RuntimeProbes().probe().labels))
        self._last_server_clock: int = (
            0  # Server expects initial value to be 0 until it is set by Server.
        )

    def update_executor_status(self, value: ExecutorStatus):
        self._executor_status = value

    def update_last_server_clock(self, value: int):
        self._last_server_clock = value

    async def run(self):
        """Runs the state reporter.

        Never raises any exceptions.
        """
        # TODO: Move this method into a new async task and cancel it in shutdown().
        while not self._is_shutdown:
            stub = ExecutorAPIStub(await self._channel_manager.get_channel())
            while not self._is_shutdown:
                try:
                    # The periodic state reports serve as channel health monitoring requests
                    # (same as TCP keep-alive). Channel Manager returns the same healthy channel
                    # for all RPCs that we do from Executor to Server. So all the RPCs benefit
                    # from this channel health monitoring.
                    await self.report_state(stub)
                    await asyncio.sleep(self._reporting_interval_sec)
                except Exception as e:
                    self._logger.error(
                        f"failed to report state to the server, reconnecting in {_REPORT_BACKOFF_ON_ERROR_SEC} sec.",
                        exc_info=e,
                    )
                    await asyncio.sleep(_REPORT_BACKOFF_ON_ERROR_SEC)
                    break

        self._logger.info("state reporter shutdown")

    async def report_state(self, stub: ExecutorAPIStub):
        """Reports the current state to the server represented by the supplied stub.

        Raises exceptions on failure.
        """
        if self._total_host_resources is None:
            # We need to fetch total resources only once, because they are not changing.
            total_host_resources: HostResources = (
                await self._host_resources_provider.total_host_resources(self._logger)
            )
            total_function_executor_resources: HostResources = (
                await self._host_resources_provider.total_function_executor_resources(
                    self._logger
                )
            )
            self._logger.info(
                "detected host resources",
                total_host_resources=total_host_resources,
                total_function_executor_resources=total_function_executor_resources,
            )
            self._total_host_resources = _host_resources_to_proto(total_host_resources)
            self._total_function_executor_resources = _host_resources_to_proto(
                total_function_executor_resources
            )

        with (
            metric_state_report_errors.count_exceptions(),
            metric_state_report_latency.time(),
        ):
            metric_state_report_rpcs.inc()
            state = ExecutorState(
                executor_id=self._executor_id,
                development_mode=self._development_mode,
                hostname=self._hostname,
                flavor=_to_grpc_executor_flavor(self._flavor, self._logger),
                version=self._version,
                status=self._executor_status,
                total_function_executor_resources=self._total_function_executor_resources,
                total_resources=self._total_host_resources,
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

    async def shutdown(self):
        """Shuts down the state reporter.

        Never raises any exceptions.
        """
        self._is_shutdown = True

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


def _to_grpc_allowed_functions(function_allowlist: Optional[List[FunctionURI]]):
    if function_allowlist is None:
        return []

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
    FunctionExecutorStatus.STARTING_UP: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_STARTING_UP,
    FunctionExecutorStatus.STARTUP_FAILED_CUSTOMER_ERROR: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_CUSTOMER_ERROR,
    FunctionExecutorStatus.STARTUP_FAILED_PLATFORM_ERROR: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_PLATFORM_ERROR,
    FunctionExecutorStatus.IDLE: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_IDLE,
    FunctionExecutorStatus.RUNNING_TASK: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_RUNNING_TASK,
    FunctionExecutorStatus.UNHEALTHY: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_UNHEALTHY,
    FunctionExecutorStatus.DESTROYING: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_STOPPING,
    FunctionExecutorStatus.DESTROYED: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_STOPPED,
    FunctionExecutorStatus.SHUTDOWN: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_SHUTDOWN,
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


_FLAVOR_MAPPING = {
    ExecutorFlavor.OSS: ExecutorFlavorProto.EXECUTOR_FLAVOR_OSS,
    ExecutorFlavor.PLATFORM: ExecutorFlavorProto.EXECUTOR_FLAVOR_PLATFORM,
}


def _to_grpc_executor_flavor(
    flavor: ExecutorFlavor, logger: Any
) -> ExecutorFlavorProto:
    result: ExecutorFlavorProto = _FLAVOR_MAPPING.get(
        flavor, ExecutorFlavorProto.EXECUTOR_FLAVOR_UNKNOWN
    )

    if result == ExecutorFlavorProto.EXECUTOR_FLAVOR_UNKNOWN:
        logger.error("unexpected Executor flavor", flavor=flavor)

    return result


def _label_values_to_strings(labels: Dict[str, Any]) -> Dict[str, str]:
    return {k: str(v) for k, v in labels.items()}


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
    else:
        return GPUModelProto.GPU_MODEL_UNKNOWN
