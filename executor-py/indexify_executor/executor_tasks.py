import asyncio
from .api_objects import Task
from .content_downloader import Downloader
from .function_worker import FunctionWorker
from pydantic import Json

class DownloadGraphTask(asyncio.Task):
    def __init__(
        self,
        *,
        task: Task,
        downloader: Downloader,
        **kwargs,
    ):
        kwargs["name"] = "download_graph"
        kwargs["loop"] = asyncio.get_event_loop()
        super().__init__(
            downloader.download_graph(task.namespace, task.compute_graph),
            **kwargs,
        )
        self.task = task

class DownloadInputTask(asyncio.Task):
    def __init__(
        self,
        *,
        task: Task,
        downloader: Downloader,
        **kwargs,
    ):
        kwargs["name"] = "download_input"
        kwargs["loop"] = asyncio.get_event_loop()
        super().__init__(
            downloader.download_input(task),
            **kwargs,
        )
        self.task_id = task.id

class ExtractTask(asyncio.Task):
    def __init__(
        self,
        *,
        function_worker: FunctionWorker,
        task: Task,
        input: Json,
        code_path: str,
        **kwargs,
    ):
        kwargs["name"] = "run_function"
        kwargs["loop"] = asyncio.get_event_loop()
        super().__init__(
            function_worker.async_submit(
                namespace=task.namespace,
                graph_name=task.compute_graph,
                fn_name=task.compute_fn,
                input=input,
                code_path=code_path,
            ),
            **kwargs,
        )
        self.task_id = task.id
        self.compute_graph = task.compute_graph
        self.compute_fn = task.compute_fn
        self.namespace = task.namespace

