import asyncio
import os
from typing import Any, Optional

import httpx
import nanoid
from tensorlake.function_executor.proto.function_executor_pb2 import SerializedObject
from tensorlake.utils.http_client import get_httpx_client

from .api_objects import Task
from .metrics.downloader import (
    metric_graph_download_errors,
    metric_graph_download_latency,
    metric_graph_downloads,
    metric_graphs_from_cache,
    metric_reducer_init_value_download_errors,
    metric_reducer_init_value_download_latency,
    metric_reducer_init_value_downloads,
    metric_task_input_download_errors,
    metric_task_input_download_latency,
    metric_task_input_downloads,
    metric_tasks_downloading_graphs,
    metric_tasks_downloading_inputs,
    metric_tasks_downloading_reducer_init_value,
)


class Downloader:
    def __init__(
        self, code_path: str, base_url: str, config_path: Optional[str] = None
    ):
        self.code_path = code_path
        self._base_url = base_url
        self._client = get_httpx_client(config_path, make_async=True)

    async def download_graph(
        self, namespace: str, graph_name: str, graph_version: str, logger: Any
    ) -> SerializedObject:
        logger = logger.bind(module=__name__)
        with (
            metric_graph_download_errors.count_exceptions(),
            metric_tasks_downloading_graphs.track_inprogress(),
            metric_graph_download_latency.time(),
        ):
            metric_graph_downloads.inc()
            return await self._download_graph(
                namespace=namespace,
                graph_name=graph_name,
                graph_version=graph_version,
                logger=logger,
            )

    async def download_input(
        self,
        namespace: str,
        graph_name: str,
        graph_invocation_id: str,
        input_key: str,
        logger: Any,
    ) -> SerializedObject:
        logger = logger.bind(module=__name__)
        with (
            metric_task_input_download_errors.count_exceptions(),
            metric_tasks_downloading_inputs.track_inprogress(),
            metric_task_input_download_latency.time(),
        ):
            metric_task_input_downloads.inc()
            return await self._download_input(
                namespace=namespace,
                graph_name=graph_name,
                graph_invocation_id=graph_invocation_id,
                input_key=input_key,
                logger=logger,
            )

    async def download_init_value(
        self,
        namespace: str,
        graph_name: str,
        function_name: str,
        graph_invocation_id: str,
        reducer_output_key: str,
        logger: Any,
    ) -> SerializedObject:
        logger = logger.bind(module=__name__)
        with (
            metric_reducer_init_value_download_errors.count_exceptions(),
            metric_tasks_downloading_reducer_init_value.track_inprogress(),
            metric_reducer_init_value_download_latency.time(),
        ):
            metric_reducer_init_value_downloads.inc()
            return await self._fetch_function_init_value(
                namespace=namespace,
                graph_name=graph_name,
                function_name=function_name,
                graph_invocation_id=graph_invocation_id,
                reducer_output_key=reducer_output_key,
                logger=logger,
            )

    async def _download_graph(
        self, namespace: str, graph_name: str, graph_version: str, logger: Any
    ) -> SerializedObject:
        # Cache graph to reduce load on the server.
        graph_path = os.path.join(
            self.code_path,
            "graph_cache",
            namespace,
            graph_name,
            graph_version,
        )
        # Filesystem operations are synchronous.
        # Run in a separate thread to not block the main event loop.
        graph: Optional[SerializedObject] = await asyncio.to_thread(
            self._read_cached_graph, graph_path
        )
        if graph is not None:
            metric_graphs_from_cache.inc()
            return graph

        graph: SerializedObject = await self._fetch_graph(
            namespace=namespace,
            graph_name=graph_name,
            graph_version=graph_version,
            logger=logger,
        )
        # Filesystem operations are synchronous.
        # Run in a separate thread to not block the main event loop.
        # We don't need to wait for the write completion so we use create_task.
        asyncio.create_task(
            asyncio.to_thread(self._write_cached_graph, graph_path, graph)
        )

        return graph

    def _read_cached_graph(self, path: str) -> Optional[SerializedObject]:
        if not os.path.exists(path):
            return None

        with open(path, "rb") as f:
            return SerializedObject.FromString(f.read())

    def _write_cached_graph(self, path: str, graph: SerializedObject) -> None:
        if os.path.exists(path):
            # Another task already cached the graph.
            return None

        tmp_path = os.path.join(self.code_path, "task_graph_cache", nanoid.generate())
        os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
        with open(tmp_path, "wb") as f:
            f.write(graph.SerializeToString())
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Atomically rename the fully written file at tmp path.
        # This allows us to not use any locking because file link/unlink
        # are atomic operations at filesystem level.
        # This also allows to share the same cache between multiple Executors.
        os.replace(tmp_path, path)

    async def _download_input(
        self,
        namespace: str,
        graph_name: str,
        graph_invocation_id: str,
        input_key: str,
        logger: Any,
    ) -> SerializedObject:
        first_function_in_graph = graph_invocation_id == input_key.split("|")[-1]
        if first_function_in_graph:
            # The first function in Graph gets its input from graph invocation payload.
            return await self._fetch_graph_invocation_payload(
                namespace=namespace,
                graph_name=graph_name,
                graph_invocation_id=graph_invocation_id,
                logger=logger,
            )
        else:
            return await self._fetch_function_input(input_key=input_key, logger=logger)

    async def _fetch_graph(
        self, namespace: str, graph_name: str, graph_version: str, logger: Any
    ) -> SerializedObject:
        """Downloads the compute graph for the task and returns it."""
        return await self._fetch_url(
            url=f"{self._base_url}/internal/namespaces/{namespace}/compute_graphs/{graph_name}/versions/{graph_version}/code",
            resource_description=f"compute graph: {graph_name}",
            logger=logger,
        )

    async def _fetch_graph_invocation_payload(
        self, namespace: str, graph_name: str, graph_invocation_id: str, logger: Any
    ) -> SerializedObject:
        return await self._fetch_url(
            url=f"{self._base_url}/namespaces/{namespace}/compute_graphs/{graph_name}/invocations/{graph_invocation_id}/payload",
            resource_description=f"graph invocation payload: {graph_invocation_id}",
            logger=logger,
        )

    async def _fetch_function_input(
        self, input_key: str, logger: Any
    ) -> SerializedObject:
        return await self._fetch_url(
            url=f"{self._base_url}/internal/fn_outputs/{input_key}",
            resource_description=f"function input: {input_key}",
            logger=logger,
        )

    async def _fetch_function_init_value(
        self,
        namespace: str,
        graph_name: str,
        function_name: str,
        graph_invocation_id: str,
        reducer_output_key: str,
        logger: Any,
    ) -> SerializedObject:
        return await self._fetch_url(
            url=f"{self._base_url}/namespaces/{namespace}/compute_graphs/{graph_name}"
            f"/invocations/{graph_invocation_id}/fn/{function_name}/output/{reducer_output_key}",
            resource_description=f"reducer output: {reducer_output_key}",
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
