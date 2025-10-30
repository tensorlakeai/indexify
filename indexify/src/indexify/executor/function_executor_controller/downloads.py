import asyncio
import os
from pathlib import Path
from typing import Any

import nanoid
from tensorlake.function_executor.proto.function_executor_pb2 import (
    SerializedObject,
)

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.proto.executor_api_pb2 import (
    FunctionExecutorDescription,
)

from .blob_utils import serialized_object_manifest_from_data_payload
from .metrics.downloads import (
    metric_application_download_errors,
    metric_application_download_latency,
    metric_application_downloads,
    metric_applications_from_cache,
)


async def download_application_code(
    function_executor_description: FunctionExecutorDescription,
    cache_path: Path,
    blob_store: BLOBStore,
    logger: Any,
) -> SerializedObject:
    logger = logger.bind(module=__name__)
    with (
        metric_application_download_errors.count_exceptions(),
        metric_application_download_latency.time(),
    ):
        metric_application_downloads.inc()
        return await _download_application_code(
            function_executor_description=function_executor_description,
            cache_path=cache_path,
            blob_store=blob_store,
            logger=logger,
        )


async def _download_application_code(
    function_executor_description: FunctionExecutorDescription,
    cache_path: Path,
    blob_store: BLOBStore,
    logger: Any,
) -> SerializedObject:
    # Cache application to reduce load on the server.
    application_path = os.path.join(
        str(cache_path),
        "application_cache",
        function_executor_description.function.namespace,
        function_executor_description.function.application_name,
        function_executor_description.function.application_version,
    )
    # Filesystem operations are synchronous.
    # Run in a separate thread to not block the main event loop.
    application: SerializedObject | None = await asyncio.to_thread(
        _read_cached_application, path=application_path
    )
    if application is not None:
        metric_applications_from_cache.inc()
        return application

    data: bytes = await blob_store.get(
        uri=function_executor_description.application.uri, logger=logger
    )
    application: SerializedObject = SerializedObject(
        manifest=serialized_object_manifest_from_data_payload(
            function_executor_description.application
        ),
        data=data,
    )

    # Filesystem operations are synchronous.
    # Run in a separate thread to not block the main event loop.
    # We don't need to wait for the write completion so we use create_task.
    asyncio.create_task(
        asyncio.to_thread(
            _write_cached_application,
            path=application_path,
            application=application,
            cache_path=cache_path,
        ),
        name="application cache write",
    )

    return application


def _read_cached_application(path: str) -> SerializedObject | None:
    if not os.path.exists(path):
        return None

    with open(path, "rb") as f:
        return SerializedObject.FromString(f.read())


def _write_cached_application(
    path: str, application: SerializedObject, cache_path: Path
) -> None:
    if os.path.exists(path):
        # Another allocation already cached the application.
        return None

    tmp_path = os.path.join(
        str(cache_path), "task_application_cache", nanoid.generate()
    )
    os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
    with open(tmp_path, "wb") as f:
        f.write(application.SerializeToString())
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Atomically rename the fully written file at tmp path.
    # This allows us to not use any locking because file link/unlink
    # are atomic operations at filesystem level.
    # This also allows to share the same cache between multiple Executors.
    os.replace(tmp_path, path)
