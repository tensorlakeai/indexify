import asyncio
from typing import Any, Dict, List, Optional

import grpc

from indexify.proto.task_scheduler_pb2 import (
    AllowedFunction,
    ExecutorState,
    ExecutorStatus,
    FunctionExecutorDescription,
)
from indexify.proto.task_scheduler_pb2 import (
    FunctionExecutorState as FunctionExecutorStateProto,
)
from indexify.proto.task_scheduler_pb2 import (
    FunctionExecutorStatus as FunctionExecutorStatusProto,
)
from indexify.proto.task_scheduler_pb2 import (
    GPUModel,
    GPUResources,
    HostResources,
    ReportExecutorStateRequest,
)
from indexify.proto.task_scheduler_pb2_grpc import (
    TaskSchedulerServiceStub,
)

from ..api_objects import FunctionURI
from ..function_executor.function_executor_state import FunctionExecutorState
from ..function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from ..function_executor.function_executor_status import FunctionExecutorStatus
from .channel_creator import ChannelCreator
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
        development_mode: bool,
        function_allowlist: Optional[List[FunctionURI]],
        function_executor_states: FunctionExecutorStatesContainer,
        channel_creator: ChannelCreator,
        logger: Any,
    ):
        self._executor_id: str = executor_id
        self._development_mode: bool = development_mode
        self._function_executor_states: FunctionExecutorStatesContainer = (
            function_executor_states
        )
        self._channel_creator = channel_creator
        self._logger: Any = logger.bind(module=__name__)
        self._is_shutdown: bool = False
        self._executor_status: ExecutorStatus = ExecutorStatus.EXECUTOR_STATUS_UNKNOWN
        self._allowed_functions: List[AllowedFunction] = _to_grpc_allowed_functions(
            function_allowlist
        )

    def update_executor_status(self, value: ExecutorStatus):
        self._executor_status = value

    async def run(self):
        """Runs the state reporter.

        Never raises any exceptions.
        """
        while not self._is_shutdown:
            async with await self._channel_creator.create() as server_channel:
                server_channel: grpc.aio.Channel
                stub = TaskSchedulerServiceStub(server_channel)
                while not self._is_shutdown:
                    try:
                        await self._report_state(stub)
                        await asyncio.sleep(_REPORTING_INTERVAL_SEC)
                    except Exception as e:
                        self._logger.error(
                            f"Failed to report state to the server, reconnecting in {_REPORT_BACKOFF_ON_ERROR_SEC} sec.",
                            exc_info=e,
                        )
                        await asyncio.sleep(_REPORT_BACKOFF_ON_ERROR_SEC)
                        break

        self._logger.info("State reporter shutdown")

    async def _report_state(self, stub: TaskSchedulerServiceStub):
        with (
            metric_state_report_errors.count_exceptions(),
            metric_state_report_latency.time(),
        ):
            metric_state_report_rpcs.inc()
            state = ExecutorState(
                executor_id=self._executor_id,
                development_mode=self._development_mode,
                executor_status=self._executor_status,
                free_resources=await self._fetch_free_host_resources(),
                allowed_functions=self._allowed_functions,
                function_executor_states=await self._fetch_function_executor_states(),
            )

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
