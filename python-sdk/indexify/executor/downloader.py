import os
from typing import Optional

import httpx
import structlog
from pydantic import BaseModel

from indexify.functions_sdk.data_objects import IndexifyData

from ..common_util import get_httpx_client
from ..functions_sdk.object_serializer import JsonSerializer, get_serializer
from .api_objects import Task

logger = structlog.get_logger(module=__name__)


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

        logger.info(
            "downloading graph", namespace=namespace, name=name, version=version
        )
        response = self._client.get(
            f"{self.base_url}/internal/namespaces/{namespace}/compute_graphs/{name}/code"
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error(
                "failed to download graph",
                namespace=namespace,
                name=name,
                version=version,
                error=response.text,
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

        logger.info("downloading input", url=url, reducer_url=reducer_url)
        response = self._client.get(url)

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error(
                "failed to download input",
                url=url,
                reducer_url=reducer_url,
                error=response.text,
            )
            raise

        encoder = (
            "json"
            if response.headers["content-type"] == JsonSerializer.content_type
            else "cloudpickle"
        )
        if task.invocation_id == input_id:
            return DownloadedInputs(
                input=IndexifyData(
                    payload=response.content, id=input_id, encoder=encoder
                ),
            )

        input_payload = response.content

        if reducer_url:
            response = self._client.get(reducer_url)
            try:
                response.raise_for_status()
                init_value = response.content
            except httpx.HTTPStatusError as e:
                logger.error(
                    "failed to download reducer output",
                    url=reducer_url,
                    error=response.text,
                )
                raise
            return DownloadedInputs(
                input=IndexifyData(
                    input_id=task.invocation_id,
                    payload=input_payload,
                    encoder=encoder,
                ),
                init_value=IndexifyData(
                    input_id=task.invocation_id, payload=init_value, encoder=encoder
                ),
            )

        return DownloadedInputs(
            input=IndexifyData(
                input_id=task.invocation_id,
                payload=input_payload,
                encoder=encoder,
            )
        )
