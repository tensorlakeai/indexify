import asyncio

from indexify.function_executor.proto.function_executor_pb2 import (
    RunTaskRequest,
)

from .api_objects import Task
from .function_executor_request_creator import FunctionExecutorRequestCreator
from .function_worker import FunctionWorker


class CreateRunFunctionRequestTask(asyncio.Task):
    def __init__(
        self,
        *,
        task: Task,
        request_creator: FunctionExecutorRequestCreator,
        **kwargs,
    ):
        kwargs["name"] = "create_run_function_request"
        kwargs["loop"] = asyncio.get_event_loop()
        super().__init__(
            request_creator.create(task),
            **kwargs,
        )
        self.task = task


class RunFunctionTask(asyncio.Task):
    def __init__(
        self,
        *,
        function_worker: FunctionWorker,
        task: Task,
        request: RunTaskRequest,
        **kwargs,
    ):
        kwargs["name"] = "run_function"
        kwargs["loop"] = asyncio.get_event_loop()
        super().__init__(
            function_worker.run(request),
            **kwargs,
        )
        self.task = task
