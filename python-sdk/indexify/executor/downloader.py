import os
import httpx
from .api_objects import Task
from rich import print


class Downloader:
    def __init__(self, code_path: str, base_url: str):
        self.code_path = code_path
        self.base_url = base_url

    async def download_graph(self, namespace: str, name: str):
        path = os.path.join(self.code_path, namespace, name)
        if os.path.exists(path):
            return path
        print(f"[bold] downloader: [/bold] downloading graph: {name} to path: {path}")
        response = httpx.get(
            f"{self.base_url}/internal/namespaces/{namespace}/compute_graphs/{name}/code"
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(
                f"[bold red] downloader: [/bold] failed to download graph {name} with error {response.text}"
            )
            raise
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(response.content)
        return path

    async def download_input(self, task: Task) -> bytes:
        input_id = task.input_key.split("|")[-1]
        if task.invocation_id == input_id:
            url = f"{self.base_url}/namespaces/{task.namespace}/compute_graphs/{task.compute_graph}/invocations/{task.invocation_id}/payload"
        else:
            url = f"{self.base_url}/internal/fn_outputs/{task.input_key}"

        print(f"[bold] downloader: [/bold] downloading input from url {url}")
        response = httpx.get(url)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(
                f"[bold red] downloader: [/bold] failed to download input {task.input_key} with error {response.text}"
            )
            raise
        return response.content
