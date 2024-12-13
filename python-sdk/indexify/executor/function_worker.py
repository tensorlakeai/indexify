import asyncio
from typing import Any, Dict, Optional

import grpc
import structlog

from indexify.function_executor.proto.function_executor_pb2 import (
    FunctionOutput,
    InitializeRequest,
    RouterOutput,
    RunTaskRequest,
    RunTaskResponse,
    SerializedObject,
)
from indexify.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

from .api_objects import Task
from .downloader import DownloadedInputs
from .function_executor.function_executor import FunctionExecutor
from .function_executor.function_executor_factory import (
    FunctionExecutorFactory,
)
from .function_executor.function_executor_map import FunctionExecutorMap


class FunctionWorkerInput:
    """Task with all the resources required to run it."""

    def __init__(
        self,
        task: Task,
        graph: Optional[SerializedObject] = None,
        function_input: Optional[DownloadedInputs] = None,
    ):
        self.task = task
        # Must not be None when running the task.
        self.graph = graph
        # Must not be None when running the task.
        self.function_input = function_input


class FunctionWorkerOutput:
    def __init__(
        self,
        function_output: Optional[FunctionOutput] = None,
        router_output: Optional[RouterOutput] = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        reducer: bool = False,
        success: bool = False,
    ):
        self.function_output = function_output
        self.router_output = router_output
        self.stdout = stdout
        self.stderr = stderr
        self.reducer = reducer
        self.success = success


class FunctionExecutorState:
    def __init__(
        self,
        function_id_with_version: str,
        function_id_without_version: str,
        ongoing_tasks_count: int,
    ):
        self.function_id_with_version: str = function_id_with_version
        self.function_id_without_version: str = function_id_without_version
        self.ongoing_tasks_count: int = ongoing_tasks_count


class FunctionWorker:
    def __init__(self, function_executor_factory: FunctionExecutorFactory):
        self._function_executors = FunctionExecutorMap(function_executor_factory)

    async def run(self, input: FunctionWorkerInput) -> FunctionWorkerOutput:
        logger = _logger(input.task)
        function_executor: Optional[FunctionExecutor] = None
        try:
            function_executor = await self._obtain_function_executor(input, logger)
            return await self._run_in_executor(
                function_executor=function_executor, input=input
            )
        except Exception as e:
            logger.error(
                "failed running the task",
                exc_info=e,
            )
            if function_executor is not None:
                # This will fail all the tasks concurrently running in this Function Executor. Not great.
                await self._function_executors.delete(
                    id=_function_id_without_version(input.task),
                    function_executor=function_executor,
                    logger=logger,
                )
            return _internal_error_output()

    async def _obtain_function_executor(
        self, input: FunctionWorkerInput, logger: Any
    ) -> FunctionExecutor:
        # Temporary policy for Function Executors lifecycle:
        # There can only be a single Function Executor per function.
        # If a Function Executor already exists for a different function version then wait until
        # all the tasks finish in the existing Function Executor and then destroy it first.
        initialize_request: InitializeRequest = InitializeRequest(
            namespace=input.task.namespace,
            graph_name=input.task.compute_graph,
            graph_version=input.task.graph_version,
            function_name=input.task.compute_fn,
            graph=input.graph,
        )
        initial_function_executor_state: FunctionExecutorState = FunctionExecutorState(
            function_id_with_version=_function_id_with_version(input.task),
            function_id_without_version=_function_id_without_version(input.task),
            ongoing_tasks_count=0,
        )

        while True:
            function_executor = await self._function_executors.get_or_create(
                id=_function_id_without_version(input.task),
                initialize_request=initialize_request,
                initial_state=initial_function_executor_state,
                logger=logger,
            )

            # No need to lock Function Executor state as we are not awaiting.
            function_executor_state: FunctionExecutorState = function_executor.state()
            if (
                function_executor_state.function_id_with_version
                == _function_id_with_version(input.task)
            ):
                # The existing Function Executor is for the same function version so we can run the task in it.
                # Increment the ongoing tasks count before awaiting to prevent the Function Executor from being destroyed
                # by another coroutine.
                function_executor_state.ongoing_tasks_count += 1
                return function_executor

            # This loop implements the temporary policy so it's implemented using polling instead of a lower
            # latency event based mechanism with a higher complexity.
            if function_executor_state.ongoing_tasks_count == 0:
                logger.info(
                    "destroying existing Function Executor for different function version",
                    function_id=_function_id_with_version(input.task),
                    executor_function_id=function_executor_state.function_id_with_version,
                )
                await self._function_executors.delete(
                    id=_function_id_without_version(input.task),
                    function_executor=function_executor,
                    logger=logger,
                )
            else:
                logger.info(
                    "waiting for existing Function Executor to finish",
                    function_id=_function_id_with_version(input.task),
                    executor_function_id=function_executor_state.function_id_with_version,
                )
                await asyncio.sleep(
                    5
                )  # Wait for 5 secs before checking if all tasks for the existing Function Executor finished.

    async def _run_in_executor(
        self, function_executor: FunctionExecutor, input: FunctionWorkerInput
    ) -> FunctionWorkerOutput:
        """Runs the task in the Function Executor.

        The Function Executor's ongoing_tasks_count must be incremented before calling this function.
        """
        try:
            run_task_request: RunTaskRequest = RunTaskRequest(
                graph_invocation_id=input.task.invocation_id,
                task_id=input.task.id,
                function_input=input.function_input.input,
            )
            if input.function_input.init_value is not None:
                run_task_request.function_init_value.CopyFrom(
                    input.function_input.init_value
                )
            channel: grpc.aio.Channel = await function_executor.channel()
            run_task_response: RunTaskResponse = await FunctionExecutorStub(
                channel
            ).run_task(run_task_request)
            return _to_output(run_task_response)
        finally:
            # If this Function Executor was destroyed then it's not
            # visible in the map but we still have a reference to it.
            function_executor.state().ongoing_tasks_count -= 1

    async def shutdown(self) -> None:
        await self._function_executors.clear(
            logger=structlog.get_logger(module=__name__, event="shutdown")
        )


def _to_output(response: RunTaskResponse) -> FunctionWorkerOutput:
    required_fields = [
        "stdout",
        "stderr",
        "is_reducer",
        "success",
    ]

    for field in required_fields:
        if not response.HasField(field):
            raise ValueError(f"Response is missing required field: {field}")

    output = FunctionWorkerOutput(
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


def _internal_error_output() -> FunctionWorkerOutput:
    return FunctionWorkerOutput(
        function_output=None,
        router_output=None,
        stdout=None,
        # We are not sharing internal error messages with the customer.
        # If customer code failed then we won't get any exceptions here.
        # All customer code errors are returned in the gRPC response.
        stderr="Platform failed to execute the function.",
        reducer=False,
        success=False,
    )


def _logger(task: Task) -> Any:
    return structlog.get_logger(
        module=__name__,
        namespace=task.namespace,
        graph_name=task.compute_graph,
        graph_version=task.graph_version,
        function_name=task.compute_fn,
        graph_invocation_id=task.invocation_id,
        task_id=task.id,
        function_id=_function_id_with_version(task),
    )


def _function_id_with_version(task: Task) -> str:
    return f"versioned/{task.namespace}/{task.compute_graph}/{task.graph_version}/{task.compute_fn}"


def _function_id_without_version(task: Task) -> str:
    return f"not_versioned/{task.namespace}/{task.compute_graph}/{task.compute_fn}"
