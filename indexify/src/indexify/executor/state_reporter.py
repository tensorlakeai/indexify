import asyncio
from typing import Any, List, Optional

import grpc

from indexify.task_scheduler.proto.task_scheduler_pb2 import (
    AllowedFunction,
    ExecutorState,
    ExecutorStatus,
    FunctionExecutorDescription,
)
from indexify.task_scheduler.proto.task_scheduler_pb2 import (
    FunctionExecutorState as FunctionExecutorStateProto,
)
from indexify.task_scheduler.proto.task_scheduler_pb2 import (
    GPUResources,
    HostResources,
    ReportExecutorStateRequest,
)
from indexify.task_scheduler.proto.task_scheduler_pb2_grpc import (
    TaskSchedulerServiceStub,
)

from .api_objects import FunctionURI
from .function_executor.function_executor_state import FunctionExecutorState
from .function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from .grpc.channel_creator import ChannelCreator

_REPORTING_INTERVAL_SEC = 5
_REPORT_RPC_TIMEOUT_SEC = 5
_REPORT_BACKOFF_SEC = 5


class ExecutorStateReporter:
    def __init__(
        self,
        executor_id: str,
        function_allowlist: Optional[List[FunctionURI]],
        function_executor_states: FunctionExecutorStatesContainer,
        channel_creator: ChannelCreator,
        logger: Any,
    ):
        self._executor_id: str = executor_id
        self._development_mode: bool = False
        self._function_executor_states: FunctionExecutorStatesContainer = (
            function_executor_states
        )
        self._channel_creator = channel_creator
        self._logger: Any = logger.bind(module=__name__)
        self._is_shutdown: bool = False
        self._executor_status: ExecutorStatus = ExecutorStatus.EXECUTOR_STATUS_UNKNOWN
        self._allowed_functions: List[AllowedFunction] = []

        for function_uri in (
            function_allowlist if function_allowlist is not None else []
        ):
            allowed_function = AllowedFunction(
                namespace=function_uri.namespace,
                graph_name=function_uri.compute_graph,
                function_name=function_uri.compute_fn,
            )
            if function_uri.version is not None:
                allowed_function.graph_version = function_uri.version
            self._allowed_functions.append(allowed_function)

    # TODO: Update Executor to call status updates.
    def update_status(self, value: ExecutorStatus):
        self._executor_status = value

    async def run(self):
        """Runs the state reporter.

        Never raises any exceptions.
        """
        while not self._is_shutdown:
            async with self._channel_creator.create() as server_channel:
                server_channel: grpc.aio.Channel
                stub = TaskSchedulerServiceStub(server_channel)
                while not self._is_shutdown:
                    try:
                        await self._report_state(stub)
                        await asyncio.sleep(_REPORTING_INTERVAL_SEC)
                    except Exception as e:
                        self._logger.error(
                            f"Failed to report state to the server, reconnecting in {_REPORT_BACKOFF_SEC} sec.",
                            exc_info=e,
                        )
                        await asyncio.sleep(_REPORT_BACKOFF_SEC)
                        break

        self._logger.info("State reporter shutdown.")

    async def _report_state(self, stub: TaskSchedulerServiceStub):
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
        # We're only supporting Executors with non empty function allowlist right now.
        # In this mode Server should ignore available host resources.
        # This is why it's okay to report zeros right now.
        return HostResources(
            cpu_count=0,
            memory_bytes=0,
            disk_bytes=0,
            gpu=GPUResources(
                count=0,
                model="",
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
                    status=function_executor_state.status,
                )
            )

        return states

    async def shutdown(self):
        """Shuts down the state reporter.

        Never raises any exceptions.
        """
        self._is_shutdown = True
