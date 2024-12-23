from typing import Any, Optional

import grpc

from indexify.function_executor.proto.function_executor_pb2 import (
    InitializeRequest,
    RunTaskRequest,
    RunTaskResponse,
)
from indexify.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

from ..api_objects import Task
from .function_executor import FunctionExecutor
from .function_executor_state import FunctionExecutorState
from .server.function_executor_server_factory import (
    FunctionExecutorServerConfiguration,
    FunctionExecutorServerFactory,
)
from .task_input import TaskInput
from .task_output import TaskOutput


class SingleTaskRunner:
    def __init__(
        self,
        function_executor_state: FunctionExecutorState,
        task_input: TaskInput,
        function_executor_server_factory: FunctionExecutorServerFactory,
        base_url: str,
        config_path: Optional[str],
        logger: Any,
    ):
        self._state: FunctionExecutorState = function_executor_state
        self._task_input: TaskInput = task_input
        self._factory: FunctionExecutorServerFactory = function_executor_server_factory
        self._base_url: str = base_url
        self._config_path: Optional[str] = config_path
        self._logger = logger.bind(module=__name__)

    async def run(self) -> TaskOutput:
        """Runs the task in the Function Executor.

        The FunctionExecutorState must be locked by the caller.
        The lock is released during actual task run in the server.
        The lock is relocked on return.

        Raises an exception if an error occured."""
        self._state.check_locked()

        if self._state.function_executor is None:
            self._state.function_executor = await self._create_function_executor()

        return await self._run()

    async def _create_function_executor(self) -> FunctionExecutor:
        function_executor: FunctionExecutor = FunctionExecutor(
            server_factory=self._factory, logger=self._logger
        )
        try:
            config: FunctionExecutorServerConfiguration = (
                FunctionExecutorServerConfiguration(
                    image_uri=self._task_input.task.image_uri,
                )
            )
            initialize_request: InitializeRequest = InitializeRequest(
                namespace=self._task_input.task.namespace,
                graph_name=self._task_input.task.compute_graph,
                graph_version=self._task_input.task.graph_version,
                function_name=self._task_input.task.compute_fn,
                graph=self._task_input.graph,
            )
            await function_executor.initialize(
                config=config,
                initialize_request=initialize_request,
                base_url=self._base_url,
                config_path=self._config_path,
            )
            return function_executor
        except Exception as e:
            self._logger.error(
                "failed to initialize function executor",
                exc_info=e,
            )
            await function_executor.destroy()
            raise

    async def _run(self) -> TaskOutput:
        request: RunTaskRequest = RunTaskRequest(
            graph_invocation_id=self._task_input.task.invocation_id,
            task_id=self._task_input.task.id,
            function_input=self._task_input.input,
        )
        if self._task_input.init_value is not None:
            request.function_init_value.CopyFrom(self._task_input.init_value)
        channel: grpc.aio.Channel = self._state.function_executor.channel()

        async with _RunningTaskContextManager(
            task_input=self._task_input, function_executor_state=self._state
        ):
            response: RunTaskResponse = await FunctionExecutorStub(channel).run_task(
                request
            )
            return _task_output(task=self._task_input.task, response=response)


class _RunningTaskContextManager:
    """Performs all the actions required before and after running a task."""

    def __init__(
        self, task_input: TaskInput, function_executor_state: FunctionExecutorState
    ):
        self._task_input: TaskInput = task_input
        self._state: FunctionExecutorState = function_executor_state

    async def __aenter__(self):
        self._state.increment_running_tasks()
        self._state.function_executor.invocation_state_client().add_task_to_invocation_id_entry(
            task_id=self._task_input.task.id,
            invocation_id=self._task_input.task.invocation_id,
        )
        # Unlock the state so other tasks can act depending on it.
        self._state.lock.release()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._state.lock.acquire()
        self._state.decrement_running_tasks()
        self._state.function_executor.invocation_state_client().remove_task_to_invocation_id_entry(
            task_id=self._task_input.task.id
        )


def _task_output(task: Task, response: RunTaskResponse) -> TaskOutput:
    required_fields = [
        "stdout",
        "stderr",
        "is_reducer",
        "success",
    ]

    for field in required_fields:
        if not response.HasField(field):
            raise ValueError(f"Response is missing required field: {field}")

    output = TaskOutput(
        task=task,
        stdout=response.stdout,
        stderr=response.stderr,
        reducer=response.is_reducer,
        success=response.success,
    )

    if response.HasField("function_output"):
        output.function_output = response.function_output
    if response.HasField("router_output"):
        output.router_output = response.router_output

    return output
