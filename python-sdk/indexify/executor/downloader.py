import os

import httpx
from rich import print
from rich.console import Console
from rich.panel import Panel
from rich.theme import Theme

from indexify.functions_sdk.cbor_serializer import CborSerializer
from indexify.functions_sdk.data_objects import IndexifyData

from .api_objects import Task

custom_theme = Theme(
    {
        "info": "cyan",
        "warning": "yellow",
        "error": "red",
    }
)

console = Console(theme=custom_theme)


class Downloader:
    def __init__(self, code_path: str, base_url: str):
        self.code_path = code_path
        self.base_url = base_url

    async def download_graph(self, namespace: str, name: str):
        path = os.path.join(self.code_path, namespace, name)
        if os.path.exists(path):
            return path

        console.print(
            Panel(
                f"Downloading graph: {name}\nPath: {path}",
                title="Downloader",
                border_style="cyan",
            )
        )

        response = httpx.get(
            f"{self.base_url}/internal/namespaces/{namespace}/compute_graphs/{name}/code"
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            console.print(
                Panel(
                    f"Failed to download graph: {name}\nError: {response.text}",
                    title="Downloader Error",
                    border_style="error",
                )
            )
            raise

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(response.content)
        return path

    async def download_input(self, task: Task) -> IndexifyData:
        input_id = task.input_key.split("|")[-1]
        if task.invocation_id == input_id:
            url = f"{self.base_url}/namespaces/{task.namespace}/compute_graphs/{task.compute_graph}/invocations/{task.invocation_id}/payload"
        else:
            url = f"{self.base_url}/internal/fn_outputs/{task.input_key}"

        console.print(
            Panel(
                f"Downloading input\nURL: {url}",
                title="Downloader",
                border_style="cyan",
            )
        )

        response = httpx.get(url)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            console.print(
                Panel(
                    f"Failed to download input: {task.input_key}\nError: {response.text}",
                    title="Downloader Error",
                    border_style="error",
                )
            )
            raise

        if task.invocation_id == input_id:
            return IndexifyData(payload=response.content, id=input_id)
        return CborSerializer.deserialize(response.content)
