import asyncio
import time
from typing import Any, List, Optional

from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    BLOBChunk,
    FunctionInputs,
    SerializedObjectInsideBLOB,
)

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.proto.executor_api_pb2 import DataPayload, Task

from .downloads import serialized_object_manifest_from_data_payload_proto
from .events import TaskPreparationFinished
from .metrics.prepare_task import (
    metric_task_preparation_errors,
    metric_task_preparation_latency,
    metric_task_preparations,
    metric_tasks_getting_prepared,
)
from .task_info import TaskInfo
from .task_input import TaskInput

# The following constants are subject to S3 limits,
# see https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html.
#
# 7 days - max presigned URL validity duration and limit on max function duration
_MAX_PRESIGNED_URI_EXPIRATION_SEC: int = 7 * 24 * 60 * 60
# This chunk size gives the best performance with S3. Based on our benchmarking.
_BLOB_OPTIMAL_CHUNK_SIZE_BYTES: int = 100 * 1024 * 1024  # 100 MB
# Max output size with optimal chunks is 100 * 100 MB = 10 GB.
# Each chunk requires a separate S3 presign operation, so we limit the number of optimal chunks to 100.
# S3 presign operations are local, it typically takes 30 ms per 100 URLs.
_OUTPUT_BLOB_OPTIMAL_CHUNKS_COUNT: int = 100
# This chunk size gives ~20% slower performance with S3 compared to optimal. Based on our benchmarking.
_OUTPUT_BLOB_SLOWER_CHUNK_SIZE_BYTES: int = 1 * 1024 * 1024 * 1024  # 1 GB
# Max output size with slower chunks is 100 * 1 GB = 100 GB.
_OUTPUT_BLOB_SLOWER_CHUNKS_COUNT: int = 100
# Invocation error output is using a single chunk.
_INVOCATION_ERROR_MAX_SIZE_BYTES: int = 10 * 1024 * 1024  # 10 MB


async def prepare_task(
    task_info: TaskInfo, blob_store: BLOBStore, logger: Any
) -> TaskPreparationFinished:
    """Prepares the task for execution.

    If successful then the task is runnable.
    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)
    start_time = time.monotonic()
    try:
        with (
            metric_task_preparation_errors.count_exceptions(),
            metric_tasks_getting_prepared.track_inprogress(),
            metric_task_preparation_latency.time(),
        ):
            metric_task_preparations.inc()
            task_info.input = await _prepare_task_input(
                task_info=task_info,
                blob_store=blob_store,
                logger=logger,
            )
            logger.info(
                "task was prepared for execution",
                duration=time.monotonic() - start_time,
            )
            return TaskPreparationFinished(
                task_info=task_info,
                is_success=True,
            )
    except asyncio.CancelledError:
        return TaskPreparationFinished(task_info=task_info, is_success=False)
    except BaseException as e:
        logger.error(
            "failed to prepare task for execution",
            exc_info=e,
            duration=time.monotonic() - start_time,
        )
        return TaskPreparationFinished(task_info=task_info, is_success=False)


async def _prepare_task_input(
    task_info: TaskInfo, blob_store: BLOBStore, logger: Any
) -> TaskInput:
    """Prepares the task for execution.

    Raises an exception on error.
    """
    task: Task = task_info.allocation.task
    function_init_value_blob: Optional[BLOB] = None
    function_init_value: Optional[SerializedObjectInsideBLOB] = None
    if task.HasField("reducer_input"):
        function_init_value_blob = await _presign_function_input_blob(
            data_payload=task.reducer_input,
            blob_store=blob_store,
            logger=logger,
        )
        function_init_value = _to_serialized_object_inside_blob(task.reducer_input)

    function_outputs_blob_uri: str = (
        f"{task.output_payload_uri_prefix}.{task_info.allocation.allocation_id}.output"
    )
    invocation_error_blob_uri: str = (
        f"{task.invocation_error_payload_uri_prefix}.{task.graph_invocation_id}.inverr"
    )

    # The uploads are completed when finalizing the task.
    function_outputs_blob_upload_id: Optional[str] = None
    invocation_error_blob_upload_id: Optional[str] = None

    try:
        function_outputs_blob_upload_id = await blob_store.create_multipart_upload(
            uri=function_outputs_blob_uri,
            logger=logger,
        )
        invocation_error_blob_upload_id = await blob_store.create_multipart_upload(
            uri=invocation_error_blob_uri,
            logger=logger,
        )
    except BaseException:
        if function_outputs_blob_upload_id is not None:
            await blob_store.abort_multipart_upload(
                uri=function_outputs_blob_uri,
                upload_id=function_outputs_blob_upload_id,
                logger=logger,
            )
        if invocation_error_blob_upload_id is not None:
            await blob_store.abort_multipart_upload(
                uri=invocation_error_blob_uri,
                upload_id=invocation_error_blob_upload_id,
                logger=logger,
            )
        raise

    return TaskInput(
        function_inputs=FunctionInputs(
            function_input_blob=await _presign_function_input_blob(
                data_payload=task.input,
                blob_store=blob_store,
                logger=logger,
            ),
            function_input=_to_serialized_object_inside_blob(task.input),
            function_init_value_blob=function_init_value_blob,
            function_init_value=function_init_value,
            function_outputs_blob=await _presign_function_outputs_blob(
                uri=function_outputs_blob_uri,
                upload_id=function_outputs_blob_upload_id,
                blob_store=blob_store,
                logger=logger,
            ),
            invocation_error_blob=await _presign_invocation_error_blob(
                uri=invocation_error_blob_uri,
                upload_id=invocation_error_blob_upload_id,
                blob_store=blob_store,
                logger=logger,
            ),
        ),
        function_outputs_blob_uri=function_outputs_blob_uri,
        function_outputs_blob_upload_id=function_outputs_blob_upload_id,
        invocation_error_blob_uri=invocation_error_blob_uri,
        invocation_error_blob_upload_id=invocation_error_blob_upload_id,
    )


async def _presign_function_input_blob(
    data_payload: DataPayload, blob_store: BLOBStore, logger: Any
) -> BLOB:
    get_blob_uri: str = await blob_store.presign_get_uri(
        uri=data_payload.uri,
        expires_in_sec=_MAX_PRESIGNED_URI_EXPIRATION_SEC,
        logger=logger,
    )
    chunks: List[BLOBChunk] = []

    while len(chunks) * _BLOB_OPTIMAL_CHUNK_SIZE_BYTES < data_payload.size:
        chunks.append(
            BLOBChunk(
                uri=get_blob_uri,  # The URI allows to read any byte range in the BLOB.
                size=_BLOB_OPTIMAL_CHUNK_SIZE_BYTES,
                # ETag is only set by FE when returning BLOBs to us
            )
        )

    return BLOB(
        chunks=chunks,
    )


async def _presign_function_outputs_blob(
    uri: str, upload_id: str, blob_store: BLOBStore, logger: Any
) -> BLOB:
    """Presigns the output blob for the task."""
    chunks: List[BLOBChunk] = []

    while len(chunks) != (
        _OUTPUT_BLOB_OPTIMAL_CHUNKS_COUNT + _OUTPUT_BLOB_SLOWER_CHUNKS_COUNT
    ):
        upload_chunk_uri: str = await blob_store.presign_upload_part_uri(
            uri=uri,
            part_number=len(chunks) + 1,
            upload_id=upload_id,
            expires_in_sec=_MAX_PRESIGNED_URI_EXPIRATION_SEC,
            logger=logger,
        )

        chunk_size: int = (
            _BLOB_OPTIMAL_CHUNK_SIZE_BYTES
            if len(chunks) < _OUTPUT_BLOB_OPTIMAL_CHUNKS_COUNT
            else _OUTPUT_BLOB_SLOWER_CHUNK_SIZE_BYTES
        )
        chunks.append(
            BLOBChunk(
                uri=upload_chunk_uri,
                size=chunk_size,
                # ETag is only set by FE when returning BLOBs to us
            )
        )

    return BLOB(
        chunks=chunks,
    )


async def _presign_invocation_error_blob(
    uri: str, upload_id: str, blob_store: BLOBStore, logger: Any
) -> BLOB:
    """Presigns the output blob for the invocation error."""
    upload_chunk_uri: str = await blob_store.presign_upload_part_uri(
        uri=uri,
        part_number=1,
        upload_id=upload_id,
        expires_in_sec=_MAX_PRESIGNED_URI_EXPIRATION_SEC,
        logger=logger,
    )
    return BLOB(
        chunks=[
            BLOBChunk(
                uri=upload_chunk_uri,
                size=_INVOCATION_ERROR_MAX_SIZE_BYTES,
                # ETag is only set by FE when returning BLOBs to us
            )
        ]
    )


def _to_serialized_object_inside_blob(
    data_payload: DataPayload,
) -> SerializedObjectInsideBLOB:
    return SerializedObjectInsideBLOB(
        manifest=serialized_object_manifest_from_data_payload_proto(data_payload),
        offset=data_payload.offset,
    )
