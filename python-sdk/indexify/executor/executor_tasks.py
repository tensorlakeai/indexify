import asyncio
from enum import Enum, unique
from typing import Coroutine

from .api_objects import Task


class RunFunctionTask(asyncio.Task):
    def __init__(
        self,
        *,
        task: Task,
        coroutine: Coroutine,
        loop: asyncio.AbstractEventLoop,
        **kwargs,
    ):
        kwargs["name"] = TaskEnum.RUN_FUNCTION_TASK.value
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
        coroutine: Coroutine,
        name: str,
        loop: asyncio.AbstractEventLoop,
        **kwargs,
    ):
        if not isinstance(name, TaskEnum):
            raise ValueError(f"name '{name}' must be TaskEnum")
        kwargs["name"] = name.value
        kwargs["loop"] = loop
        super().__init__(
            coroutine,
            **kwargs,
        )
        self.task = task


@unique
class TaskEnum(Enum):
    GET_RUNNABLE_TASK = "get_runnable_tasks"
    RUN_FUNCTION_TASK = "run_function"
    DOWNLOAD_GRAPH_TASK = "download_graph"
    DOWNLOAD_INPUT_TASK = "download_input"

    @classmethod
    def from_value(cls, value):
        for task_name in cls:
            if task_name.value == value:
                return task_name
        raise ValueError(f"No task found with value {value}")
