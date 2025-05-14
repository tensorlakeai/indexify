import asyncio
import os
from typing import Any, Optional

import nanoid
from tensorlake.function_executor.proto.function_executor_pb2 import SerializedObject
from tensorlake.function_executor.proto.message_validator import MessageValidator

from indexify.proto.executor_api_pb2 import DataPayload, DataPayloadEncoding

from .blob_store.blob_store import BLOBStore
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
        self,
        code_path: str,
        blob_store: BLOBStore,
    ):
        self._code_path = code_path
        self._blob_store: BLOBStore = blob_store

    async def download_graph(
        self,
        namespace: str,
        graph_name: str,
        graph_version: str,
        data_payload: DataPayload,
        logger: Any,
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
                data_payload=data_payload,
                logger=logger,
            )

    async def download_input(
        self,
        data_payload: DataPayload,
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
                data_payload=data_payload,
                logger=logger,
            )

    async def download_init_value(
        self,
        data_payload: DataPayload,
        logger: Any,
    ) -> SerializedObject:
        logger = logger.bind(module=__name__)
        with (
            metric_reducer_init_value_download_errors.count_exceptions(),
            metric_tasks_downloading_reducer_init_value.track_inprogress(),
            metric_reducer_init_value_download_latency.time(),
        ):
            metric_reducer_init_value_downloads.inc()
            return await self._download_init_value(
                data_payload=data_payload,
                logger=logger,
            )

    async def _download_graph(
        self,
        namespace: str,
        graph_name: str,
        graph_version: str,
        data_payload: DataPayload,
        logger: Any,
    ) -> SerializedObject:
        # Cache graph to reduce load on the server.
        graph_path = os.path.join(
            self._code_path,
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

        (
            MessageValidator(data_payload)
            .required_field("uri")
            .required_field("encoding")
        )
        data: bytes = await self._blob_store.get(uri=data_payload.uri, logger=logger)
        graph = _serialized_object_from_data_payload_proto(
            data_payload=data_payload,
            data=data,
        )

        # Filesystem operations are synchronous.
        # Run in a separate thread to not block the main event loop.
        # We don't need to wait for the write completion so we use create_task.
        asyncio.create_task(
            asyncio.to_thread(self._write_cached_graph, graph_path, graph),
            name="graph cache write",
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

        tmp_path = os.path.join(self._code_path, "task_graph_cache", nanoid.generate())
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
        data_payload: DataPayload,
        logger: Any,
    ) -> SerializedObject:
        (
            MessageValidator(data_payload)
            .required_field("uri")
            .required_field("encoding")
        )
        data: bytes = await self._blob_store.get(uri=data_payload.uri, logger=logger)
        return _serialized_object_from_data_payload_proto(
            data_payload=data_payload,
            data=data,
        )

    async def _download_init_value(
        self,
        data_payload: DataPayload,
        logger: Any,
    ) -> SerializedObject:
        (
            MessageValidator(data_payload)
            .required_field("uri")
            .required_field("encoding")
        )
        data: bytes = await self._blob_store.get(uri=data_payload.uri, logger=logger)
        return _serialized_object_from_data_payload_proto(
            data_payload=data_payload,
            data=data,
        )


def _serialized_object_from_data_payload_proto(
    data_payload: DataPayload, data: bytes
) -> SerializedObject:
    """Converts the given data payload and its data into SerializedObject accepted by Function Executor.

    Raises ValueError if the supplied data payload can't be converted into serialized object.
    """
    if data_payload.encoding == DataPayloadEncoding.DATA_PAYLOAD_ENCODING_BINARY_PICKLE:
        return SerializedObject(
            bytes=data,
            content_type="application/octet-stream",
        )
    elif data_payload.encoding == DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_TEXT:
        return SerializedObject(
            content_type="text/plain",
            string=data.decode("utf-8"),
        )
    elif data_payload.encoding == DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_JSON:
        result = SerializedObject(
            content_type="application/json",
            string=data.decode("utf-8"),
        )
        return result

    raise ValueError(
        f"Can't convert data payload {data_payload} into serialized object"
    )
