import asyncio
import hashlib
from socket import gethostname
from typing import Any, Dict, List, Optional

import grpc

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
from indexify.proto.executor_api_pb2 import (
    GPUModel,
    GPUResources,
    HostResources,
    ReportExecutorStateRequest,
)
from indexify.proto.executor_api_pb2_grpc import (
    ExecutorAPIStub,
)

from ..api_objects import FunctionURI
from ..executor_flavor import ExecutorFlavor
from ..function_executor.function_executor_state import FunctionExecutorState
from ..function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from ..function_executor.function_executor_status import FunctionExecutorStatus
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
        logger: Any,
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
        self._logger: Any = logger.bind(module=__name__)
        self._is_shutdown: bool = False
        self._executor_status: ExecutorStatus = ExecutorStatus.EXECUTOR_STATUS_UNKNOWN
        self._allowed_functions: List[AllowedFunction] = _to_grpc_allowed_functions(
            function_allowlist
        )
        self._labels.update(_label_values_to_strings(RuntimeProbes().probe().labels))

    def update_executor_status(self, value: ExecutorStatus):
        self._executor_status = value

    async def run(self):
        """Runs the state reporter.

        Never raises any exceptions.
        """
        while not self._is_shutdown:
            async with await self._channel_manager.get_channel() as server_channel:
                server_channel: grpc.aio.Channel
                stub = ExecutorAPIStub(server_channel)
                while not self._is_shutdown:
                    try:
                        # The periodic state reports serve as channel health monitoring requests
                        # (same as TCP keep-alive). Channel Manager returns the same healthy channel
                        # for all RPCs that we do from Executor to Server. So all the RPCs benefit
                        # from this channel health monitoring.
                        await self.report_state(stub)
                        await asyncio.sleep(_REPORTING_INTERVAL_SEC)
                    except Exception as e:
                        self._logger.error(
                            f"Failed to report state to the server, reconnecting in {_REPORT_BACKOFF_ON_ERROR_SEC} sec.",
                            exc_info=e,
                        )
                        await asyncio.sleep(_REPORT_BACKOFF_ON_ERROR_SEC)
                        break

        self._logger.info("State reporter shutdown")

    async def report_state(self, stub: ExecutorAPIStub):
        """Reports the current state to the server represented by the supplied stub.

        Raises exceptions on failure.
        """
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
                free_resources=await self._fetch_free_host_resources(),
                allowed_functions=self._allowed_functions,
                function_executor_states=await self._fetch_function_executor_states(),
                labels=self._labels,
            )
            state.state_hash = _state_hash(state)

            await stub.report_executor_state(
                ReportExecutorStateRequest(executor_state=state),
                timeout=_REPORT_RPC_TIMEOUT_SEC,
            )

    async def _fetch_free_host_resources(self) -> HostResources:
        # TODO: Implement host resource metrics reporting.
        return HostResources(
            cpu_count=0,
            memory_bytes=0,
            disk_bytes=0,
            gpu=GPUResources(
                count=0,
                model=GPUModel.GPU_MODEL_UNKNOWN,
            ),
        )

    async def _fetch_function_executor_states(self) -> List[FunctionExecutorStateProto]:
        states = []

        async for function_executor_state in self._function_executor_states:
            function_executor_state: FunctionExecutorState
            states.append(
                FunctionExecutorStateProto(
                    description=FunctionExecutorDescription(
                        id=function_executor_state.id,
                        namespace=function_executor_state.namespace,
                        graph_name=function_executor_state.graph_name,
                        graph_version=function_executor_state.graph_version,
                        function_name=function_executor_state.function_name,
                    ),
                    status=_to_grpc_function_executor_status(
                        function_executor_state.status, self._logger
                    ),
                )
            )

        return states

    async def shutdown(self):
        """Shuts down the state reporter.

        Never raises any exceptions.
        """
        self._is_shutdown = True


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
    FunctionExecutorStatus.SHUTDOWN: FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_STOPPED,
}


def _to_grpc_function_executor_status(
    status: FunctionExecutorStatus, logger: Any
) -> FunctionExecutorStatusProto:
    result: FunctionExecutorStatusProto = _STATUS_MAPPING.get(
        status, FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_UNKNOWN
    )

    if result == FunctionExecutorStatusProto.FUNCTION_EXECUTOR_STATUS_UNKNOWN:
        logger.error("Unexpected Function Executor status", status=status)

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
        logger.error("Unexpected Executor flavor", flavor=flavor)

    return result


def _label_values_to_strings(labels: Dict[str, Any]) -> Dict[str, str]:
    return {k: str(v) for k, v in labels.items()}


def _state_hash(state: ExecutorState) -> str:
    serialized_state: bytes = state.SerializeToString(deterministic=True)
    hasher = hashlib.sha256(usedforsecurity=False)
    hasher.update(serialized_state)
    return hasher.hexdigest()
