from typing import Optional

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


class FunctionWorker:
    def __init__(self, function_executor_factory: FunctionExecutorFactory):
        self._function_executors = FunctionExecutorMap(function_executor_factory)

    async def run(self, input: FunctionWorkerInput) -> FunctionWorkerOutput:
        function_id = _function_id(input.task)
        function_executor: Optional[FunctionExecutor] = None

        try:
            logger = structlog.get_logger(
                module=__name__,
                namespace=input.task.namespace,
                graph_name=input.task.compute_graph,
                graph_version=input.task.graph_version,
                function_name=input.task.compute_fn,
                graph_invocation_id=input.task.invocation_id,
                task_id=input.task.id,
            )
            run_task_request: RunTaskRequest = RunTaskRequest(
                graph_invocation_id=input.task.invocation_id,
                task_id=input.task.id,
                function_input=input.function_input.input,
            )
            if input.function_input.init_value is not None:
                run_task_request.function_init_value.CopyFrom(
                    input.function_input.init_value
                )

            function_executor = await self._function_executors.get_or_create(
                id=function_id,
                initialize_request=InitializeRequest(
                    namespace=input.task.namespace,
                    graph_name=input.task.compute_graph,
                    graph_version=input.task.graph_version,
                    function_name=input.task.compute_fn,
                    graph=input.graph,
                ),
                logger=logger,
            )
            channel: grpc.aio.Channel = await function_executor.channel()
            run_task_response: RunTaskResponse = await FunctionExecutorStub(
                channel
            ).RunTask(run_task_request)
            return _worker_output(run_task_response)
        except Exception as e:
            logger.error(
                "failed running function in Function Executor",
                exc_info=e,
            )
            if function_executor is not None:
                # This will fail all the tasks concurrently running in this Function Executor. Not great.
                await self._function_executors.delete_and_destroy(
                    id=function_id,
                    logger=logger,
                )
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

    async def shutdown(self) -> None:
        await self._function_executors.clear(
            logger=structlog.get_logger(module=__name__, event="shutdown")
        )


def _worker_output(response: RunTaskResponse) -> FunctionWorkerOutput:
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


def _function_id(task: Task) -> str:
    return (
        f"{task.namespace}/{task.compute_graph}/{task.graph_version}/{task.compute_fn}"
    )
