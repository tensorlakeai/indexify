import asyncio
import os
from pathlib import Path
from typing import Any, Optional

import nanoid
from tensorlake.function_executor.proto.function_executor_pb2 import (
    SerializedObject,
    SerializedObjectEncoding,
    SerializedObjectManifest,
)

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.proto.executor_api_pb2 import (
    DataPayload,
    DataPayloadEncoding,
    FunctionExecutorDescription,
)

from .metrics.downloads import (
    metric_graph_download_errors,
    metric_graph_download_latency,
    metric_graph_downloads,
    metric_graphs_from_cache,
)


async def download_graph(
    function_executor_description: FunctionExecutorDescription,
    cache_path: Path,
    blob_store: BLOBStore,
    logger: Any,
) -> SerializedObject:
    logger = logger.bind(module=__name__)
    with (
        metric_graph_download_errors.count_exceptions(),
        metric_graph_download_latency.time(),
    ):
        metric_graph_downloads.inc()
        return await _download_graph(
            function_executor_description=function_executor_description,
            cache_path=cache_path,
            blob_store=blob_store,
            logger=logger,
        )


async def _download_graph(
    function_executor_description: FunctionExecutorDescription,
    cache_path: Path,
    blob_store: BLOBStore,
    logger: Any,
) -> SerializedObject:
    # Cache graph to reduce load on the server.
    graph_path = os.path.join(
        str(cache_path),
        "graph_cache",
        function_executor_description.namespace,
        function_executor_description.graph_name,
        function_executor_description.graph_version,
    )
    # Filesystem operations are synchronous.
    # Run in a separate thread to not block the main event loop.
    graph: Optional[SerializedObject] = await asyncio.to_thread(
        _read_cached_graph, path=graph_path
    )
    if graph is not None:
        metric_graphs_from_cache.inc()
        return graph

    data: bytes = await blob_store.get(
        uri=function_executor_description.graph.uri, logger=logger
    )
    graph: SerializedObject = SerializedObject(
        manifest=serialized_object_manifest_from_data_payload_proto(
            function_executor_description.graph
        ),
        data=data,
    )

    # Filesystem operations are synchronous.
    # Run in a separate thread to not block the main event loop.
    # We don't need to wait for the write completion so we use create_task.
    asyncio.create_task(
        asyncio.to_thread(
            _write_cached_graph, path=graph_path, graph=graph, cache_path=cache_path
        ),
        name="graph cache write",
    )

    return graph


def _read_cached_graph(path: str) -> Optional[SerializedObject]:
    if not os.path.exists(path):
        return None

    with open(path, "rb") as f:
        return SerializedObject.FromString(f.read())


def _write_cached_graph(path: str, graph: SerializedObject, cache_path: Path) -> None:
    if os.path.exists(path):
        # Another task already cached the graph.
        return None

    tmp_path = os.path.join(str(cache_path), "task_graph_cache", nanoid.generate())
    os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
    with open(tmp_path, "wb") as f:
        f.write(graph.SerializeToString())
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Atomically rename the fully written file at tmp path.
    # This allows us to not use any locking because file link/unlink
    # are atomic operations at filesystem level.
    # This also allows to share the same cache between multiple Executors.
    os.replace(tmp_path, path)


def serialized_object_manifest_from_data_payload_proto(
    data_payload: DataPayload,
) -> SerializedObjectManifest:
    """Converts the given data payload into SerializedObjectManifest accepted by Function Executor.

    Raises ValueError if the supplied data payload can't be converted.
    """
    so_manifest: SerializedObjectManifest = SerializedObjectManifest(
        # Server currently ignores encoding version so we set it to default 0.
        encoding_version=(
            data_payload.encoding_version
            if data_payload.HasField("encoding_version")
            else 0
        ),
        sha256_hash=data_payload.sha256_hash,
        size=data_payload.size,
    )

    if data_payload.encoding == DataPayloadEncoding.DATA_PAYLOAD_ENCODING_BINARY_PICKLE:
        so_manifest.encoding = (
            SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE
        )
    elif data_payload.encoding == DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_TEXT:
        so_manifest.encoding = (
            SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT
        )
    elif data_payload.encoding == DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_JSON:
        so_manifest.encoding = (
            SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON
        )
    elif data_payload.encoding == DataPayloadEncoding.DATA_PAYLOAD_ENCODING_BINARY_ZIP:
        so_manifest.encoding = (
            SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP
        )
    else:
        raise ValueError(
            f"Can't convert data payload {data_payload} into serialized object"
        )

    return so_manifest
