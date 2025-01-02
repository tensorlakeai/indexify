import asyncio
import os
from typing import Any, Optional

import httpx
import structlog

from indexify.function_executor.proto.function_executor_pb2 import (
    SerializedObject,
)

from ..common_util import get_httpx_client
from .api_objects import Task


class Downloader:
    def __init__(
        self, code_path: str, base_url: str, config_path: Optional[str] = None
    ):
        self.code_path = code_path
        self._base_url = base_url
        self._client = get_httpx_client(config_path, make_async=True)

    async def download_graph(self, task: Task) -> SerializedObject:
        # Cache graph to reduce load on the server.
        graph_path = os.path.join(
            self.code_path,
            "graph_cache",
            task.namespace,
            f"{task.compute_graph}.{task.graph_version}",
        )
        # Filesystem operations are synchronous.
        # Run in a separate thread to not block the main event loop.
        graph: Optional[SerializedObject] = await asyncio.to_thread(
            self._read_cached_graph, graph_path
        )
        if graph is not None:
            return graph

        logger = self._task_logger(task)
        graph: SerializedObject = await self._fetch_graph(task, logger)
        # Filesystem operations are synchronous.
        # Run in a separate thread to not block the main event loop.
        # We don't need to wait for the write completion so we use create_task.
        asyncio.create_task(
            asyncio.to_thread(self._write_cached_graph, task, graph_path, graph)
        )

        return graph

    def _read_cached_graph(self, path: str) -> Optional[SerializedObject]:
        if not os.path.exists(path):
            return None

        with open(path, "rb") as f:
            return SerializedObject.FromString(f.read())

    def _write_cached_graph(
        self, task: Task, path: str, graph: SerializedObject
    ) -> None:
        if os.path.exists(path):
            # Another task already cached the graph.
            return None

        tmp_path = os.path.join(self.code_path, "task_graph_cache", task.id)
        os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
        with open(tmp_path, "wb") as f:
            f.write(graph.SerializeToString())
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Atomically rename the fully written file at tmp path.
        # This allows us to not use any locking because file link/unlink
        # are atomic operations at filesystem level.
        os.replace(tmp_path, path)

    async def download_input(self, task: Task) -> SerializedObject:
        logger = self._task_logger(task)

        first_function_in_graph = task.invocation_id == task.input_key.split("|")[-1]
        if first_function_in_graph:
            # The first function in Graph gets its input from graph invocation payload.
            return await self._fetch_graph_invocation_payload(task, logger)
        else:
            return await self._fetch_function_input(task, logger)

    async def download_init_value(self, task: Task) -> Optional[SerializedObject]:
        if task.reducer_output_id is None:
            return None

        logger = self._task_logger(task)
        return await self._fetch_function_init_value(task, logger)

    def _task_logger(self, task: Task) -> Any:
        return structlog.get_logger(
            module=__name__,
            namespace=task.namespace,
            name=task.compute_graph,
            version=task.graph_version,
            task_id=task.id,
        )

    async def _fetch_graph(self, task: Task, logger: Any) -> SerializedObject:
        """Downloads the compute graph for the task and returns it."""
        return await self._fetch_url(
            url=f"{self._base_url}/internal/namespaces/{task.namespace}/compute_graphs/{task.compute_graph}/versions/{task.graph_version}/code",
            resource_description=f"compute graph: {task.compute_graph}",
            logger=logger,
        )

    async def _fetch_graph_invocation_payload(
        self, task: Task, logger: Any
    ) -> SerializedObject:
        return await self._fetch_url(
            url=f"{self._base_url}/namespaces/{task.namespace}/compute_graphs/{task.compute_graph}/invocations/{task.invocation_id}/payload",
            resource_description=f"graph invocation payload: {task.invocation_id}",
            logger=logger,
        )

    async def _fetch_function_input(self, task: Task, logger: Any) -> SerializedObject:
        return await self._fetch_url(
            url=f"{self._base_url}/internal/fn_outputs/{task.input_key}",
            resource_description=f"function input: {task.input_key}",
            logger=logger,
        )

    async def _fetch_function_init_value(
        self, task: Task, logger: Any
    ) -> SerializedObject:
        return await self._fetch_url(
            url=f"{self._base_url}/namespaces/{task.namespace}/compute_graphs/{task.compute_graph}"
            f"/invocations/{task.invocation_id}/fn/{task.compute_fn}/output/{task.reducer_output_id}",
            resource_description=f"reducer output: {task.reducer_output_id}",
            logger=logger,
        )

    async def _fetch_url(
        self, url: str, resource_description: str, logger: Any
    ) -> SerializedObject:
        logger.info(f"fetching {resource_description}", url=url)
        response: httpx.Response = await self._client.get(url)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error(
                f"failed to download {resource_description}",
                error=response.text,
                exc_info=e,
            )
            raise

        return serialized_object_from_http_response(response)


def serialized_object_from_http_response(response: httpx.Response) -> SerializedObject:
    # We're hardcoding the content type currently used by Python SDK. It might change in the future.
    # There's no other way for now to determine if the response is a bytes or string.
    if response.headers["content-type"] == "application/octet-stream":
        return SerializedObject(
            bytes=response.content, content_type=response.headers["content-type"]
        )
    else:
        return SerializedObject(
            string=response.text, content_type=response.headers["content-type"]
        )
