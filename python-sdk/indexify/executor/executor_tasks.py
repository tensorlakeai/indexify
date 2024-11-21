import asyncio

from .api_objects import Task


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


class DownloadTask(asyncio.Task):
    def __init__(
        self,
        *,
        task: Task,
        coroutine,
        name,
        loop,
        **kwargs,
    ):
        kwargs["name"] = name
        kwargs["loop"] = loop
        super().__init__(
            coroutine,
            **kwargs,
        )
        self.task = task
