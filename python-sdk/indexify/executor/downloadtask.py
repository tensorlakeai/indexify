import asyncio

from indexify.executor.api_objects import Task


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
