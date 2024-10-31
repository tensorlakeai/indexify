import asyncio
from typing import Optional

from indexify.functions_sdk.data_objects import IndexifyData

from .api_objects import Task
from .downloader import Downloader
from .function_worker import FunctionWorker


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
            downloader.download_graph(
                task.namespace, task.compute_graph, task.graph_version
            ),
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
        self.task = task


class ExtractTask(asyncio.Task):
    def __init__(
        self,
        *,
        function_worker: FunctionWorker,
        task: Task,
        input: IndexifyData,
        init_value: Optional[IndexifyData] = None,
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
                init_value=init_value,
                code_path=code_path,
                version=task.graph_version,
                invocation_id=task.invocation_id,
            ),
            **kwargs,
        )
        self.task = task
