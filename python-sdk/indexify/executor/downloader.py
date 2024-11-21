import asyncio
import os
from typing import Optional

from pydantic import BaseModel
from rich.console import Console
from rich.panel import Panel
from rich.theme import Theme

from indexify.functions_sdk.data_objects import IndexifyData

from ..functions_sdk.object_serializer import JsonSerializer, get_serializer
from .api_objects import Task
from .executor_tasks import DownloadTask

custom_theme = Theme(
    {
        "info": "cyan",
        "warning": "yellow",
        "error": "red",
    }
)

console = Console(theme=custom_theme)


class DownloadedInputs(BaseModel):
    input: IndexifyData
    init_value: Optional[IndexifyData] = None


class Downloader:
    def __init__(
        self,
        code_path: str,
        base_url: str,
        indexify_client: IndexifyClient,
    ):
        self.code_path = code_path
        self.base_url = base_url
        self._client = indexify_client
        self._event_loop = asyncio.get_event_loop()

    def download(self, task, name):
        if name == "download_graph":
            coroutine = self.download_graph(
                task.namespace, task.compute_graph, task.graph_version
            )
        elif name == "download_input":
            coroutine = self.download_input(task)
        else:
            raise Exception("Unsupported task name")

        return DownloadTask(
            task=task, coroutine=coroutine, name=name, loop=self._event_loop
        )

    async def download_graph(self, namespace: str, name: str, version: int) -> str:
        path = os.path.join(self.code_path, namespace, f"{name}.{version}")
        if os.path.exists(path):
            return path

        console.print(
            Panel(
                f"Downloading graph: {name}\nPath: {path}",
                title="downloader",
                border_style="cyan",
            )
        )

        response = self._client.download_graph(namespace, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(response.content)
        return path

    async def download_input(self, task: Task) -> DownloadedInputs:
        input_id = task.input_key.split("|")[-1]
        if task.invocation_id == input_id:
            url = f"{self.base_url}/namespaces/{task.namespace}/compute_graphs/{task.compute_graph}/invocations/{task.invocation_id}/payload"
        else:
            url = f"{self.base_url}/internal/fn_outputs/{task.input_key}"

        reducer_url = None
        if task.reducer_output_id:
            reducer_url = f"{self.base_url}/namespaces/{task.namespace}/compute_graphs/{task.compute_graph}/invocations/{task.invocation_id}/fn/{task.compute_fn}/output/{task.reducer_output_id}"

        console.print(
            Panel(
                f"downloading input\nFunction: {task.compute_fn} \n reducer id: {task.reducer_output_id}",
                title="downloader",
                border_style="cyan",
            )
        )

        input_id = task.input_key.split("|")[-1]
        if task.invocation_id == input_id:
            response = self._client.download_fn_input(task.namespace, task.compute_graph, task.invocation_id)
        else:
            response = self._client.download_fn_output(task.input_key)

        init_value = None
        if task.reducer_output_id:
            init_value = self._client.download_reducer_input(task.namespace, task.compute_graph, task.invocation_id, task.compute_fn, task.reducer_output_id)

        encoder = (
            "json"
            if response.headers["content-type"] == JsonSerializer.content_type
            else "cloudpickle"
        )
        serializer = get_serializer(encoder)

        if task.invocation_id == input_id:
            return DownloadedInputs(
                input=IndexifyData(
                    payload=response.content, id=input_id, encoder=encoder
                ),
            )

        deserialized_content = serializer.deserialize(response.content)

        if init_value:
            init_value = serializer.deserialize(init_value.content)
            return DownloadedInputs(
                input=IndexifyData(
                    input_id=task.invocation_id,
                    payload=deserialized_content,
                    encoder=encoder,
                ),
                init_value=IndexifyData(
                    input_id=task.invocation_id, payload=init_value, encoder=encoder
                ),
            )

        return DownloadedInputs(
            input=IndexifyData(
                input_id=task.invocation_id,
                payload=deserialized_content,
                encoder=encoder,
            )
        )
