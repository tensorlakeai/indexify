import asyncio

from indexify.executor.api_objects import Task


class RunFunctionTask(asyncio.Task):
    def __init__(
        self,
        *,
        task: Task,
        coroutine,
        loop,
        **kwargs,
    ):
        kwargs["name"] = "run_function"
        kwargs["loop"] = loop
        super().__init__(
            coroutine,
            **kwargs,
        )
        self.task = task
