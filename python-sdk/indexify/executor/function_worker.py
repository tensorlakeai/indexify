from typing import Optional

import structlog

from indexify.function_executor.proto.function_executor_pb2 import (
    FunctionOutput,
    InitializeRequest,
    InitializeResponse,
    RouterOutput,
    RunTaskRequest,
    RunTaskResponse,
)
from indexify.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

from .function_executor.function_executor import FunctionExecutor
from .function_executor.function_executor_factory import (
    FunctionExecutorFactory,
)


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
        self._function_executor_factory = function_executor_factory

    async def run(
        self, initialize_request: InitializeRequest, run_task_request: RunTaskRequest
    ) -> FunctionWorkerOutput:
        logger = structlog.get_logger(
            module=__name__,
            namespace=initialize_request.namespace,
            graph_name=initialize_request.graph_name,
            graph_version=initialize_request.graph_version,
            function_name=initialize_request.function_name,
            graph_invocation_id=run_task_request.graph_invocation_id,
            task_id=run_task_request.task_id,
        )
        function_executor: Optional[FunctionExecutor] = None

        try:
            function_executor = await self._function_executor_factory.create(logger)
            with function_executor.create_channel() as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = stub.Initialize(
                    initialize_request
                )
                if not initialize_response.success:
                    raise Exception("initialize RPC failed at function executor")
                run_task_response: RunTaskResponse = stub.RunTask(run_task_request)
                return _worker_output(run_task_response)
        except Exception as e:
            logger.error(
                "failed running function in a new Function Executor process",
                exc_info=e,
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
        finally:
            await self._function_executor_factory.destroy(
                executor=function_executor, logger=logger
            )

    def shutdown(self) -> None:
        # TODO: Go over list of all existing Function Executors and release them.
        pass


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
