import asyncio

from pydantic import BaseModel

from .api_objects import Task
from .downloader import Downloader
from .function_worker import FunctionWorker, FunctionWorkerInput


class DownloadGraphTask(asyncio.Task):
    def __init__(
        self,
        *,
        function_worker_input: FunctionWorkerInput,
        downloader: Downloader,
        **kwargs,
    ):
        kwargs["name"] = "download_graph"
        kwargs["loop"] = asyncio.get_event_loop()
        super().__init__(
            downloader.download_graph(function_worker_input.task),
            **kwargs,
        )
        self.function_worker_input = function_worker_input


class DownloadInputsTask(asyncio.Task):
    def __init__(
        self,
        *,
        function_worker_input: FunctionWorkerInput,
        downloader: Downloader,
        **kwargs,
    ):
        kwargs["name"] = "download_inputs"
        kwargs["loop"] = asyncio.get_event_loop()
        super().__init__(
            downloader.download_inputs(function_worker_input.task),
            **kwargs,
        )
        self.function_worker_input = function_worker_input


class RunTask(asyncio.Task):
    def __init__(
        self,
        *,
        function_worker: FunctionWorker,
        function_worker_input: FunctionWorkerInput,
        **kwargs,
    ):
        kwargs["name"] = "run_task"
        kwargs["loop"] = asyncio.get_event_loop()
        super().__init__(
            function_worker.run(function_worker_input),
            **kwargs,
        )
        self.function_worker_input = function_worker_input
