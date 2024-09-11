import os

import httpx
from pydantic import Json

from .api_objects import Task


class Downloader:
    def __init__(self, code_path: str, base_url: str):
        self.code_path = code_path
        self.base_url = base_url

    async def download_graph(self, namespace: str, name: str):
        response = httpx.get(
            f"{self.base_url}/internal/namespaces/{namespace}/compute_graphs/{name}/code"
        )
        response.raise_for_status()
        path = os.path.join(self.code_path, namespace, name)
        print(f"Downloading graph: {name} to path: {path}")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(response.content)
        return path

    async def download_input(self, task: Task) -> Json:
        if task.invocation_id == task.input_id:
            url = f"{self.base_url}/namespaces/{task.namespace}/compute_graphs/{task.compute_graph}/invocations/{task.invocation_id}/payload"
        else:
            url = f"{self.base_url}/namespaces/{task.namespace}/compute_graphs/{task.compute_graph}/invocations/{task.invocation_id}/fn/{task.compute_fn}/{task.id}"
        response = httpx.get(url)
        response.raise_for_status()
        return response.json()
