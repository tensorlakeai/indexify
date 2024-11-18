import os
from typing import Optional

import httpx
from pydantic import BaseModel
from rich.console import Console
from rich.panel import Panel
from rich.theme import Theme

from indexify.functions_sdk.data_objects import IndexifyData

from ..common_util import get_httpx_client
from ..functions_sdk.object_serializer import JsonSerializer, get_serializer
from .api_objects import Task

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
        self, code_path: str, base_url: str, config_path: Optional[str] = None
    ):
        self.code_path = code_path
        self.base_url = base_url
        self._client = get_httpx_client(config_path)

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

        response = self._client.get(
            f"{self.base_url}/internal/namespaces/{namespace}/compute_graphs/{name}/code"
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            console.print(
                Panel(
                    f"Failed to download graph: {name}\nError: {response.text}",
                    title="downloader error",
                    border_style="error",
                )
            )
            raise

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
                f"downloading input\nURL: {url} \n reducer input URL: {reducer_url}",
                title="downloader",
                border_style="cyan",
            )
        )

        response = self._client.get(url)

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            console.print(
                Panel(
                    f"failed to download input: {task.input_key}\nError: {response.text}",
                    title="downloader error",
                    border_style="error",
                )
            )
            raise

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

        if reducer_url:
            init_value = httpx.get(reducer_url)
            try:
                init_value.raise_for_status()
            except httpx.HTTPStatusError as e:
                console.print(
                    Panel(
                        f"failed to download reducer output: {task.reducer_output_id}\nError: {init_value.text}",
                        title="downloader error",
                        border_style="error",
                    )
                )
                raise
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
