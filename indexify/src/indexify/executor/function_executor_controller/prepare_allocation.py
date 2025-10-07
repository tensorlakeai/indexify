import asyncio
import time
from typing import Any, List

from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    BLOBChunk,
    FunctionInputs,
    SerializedObjectInsideBLOB,
)

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.proto.executor_api_pb2 import Allocation, DataPayload

from .allocation_info import AllocationInfo
from .allocation_input import AllocationInput
from .downloads import serialized_object_manifest_from_data_payload_proto
from .events import AllocationPreparationFinished
from .metrics.prepare_allocation import (
    metric_allocation_preparation_errors,
    metric_allocation_preparation_latency,
    metric_allocation_preparations,
    metric_allocations_getting_prepared,
)

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
# Request error output is using a single chunk.
_REQUEST_ERROR_MAX_SIZE_BYTES: int = 10 * 1024 * 1024  # 10 MB


async def prepare_allocation(
    alloc_info: AllocationInfo, blob_store: BLOBStore, logger: Any
) -> AllocationPreparationFinished:
    """Prepares the allocation for execution.

    If successful then the allocation is runnable.
    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)
    start_time = time.monotonic()
    try:
        with (
            metric_allocation_preparation_errors.count_exceptions(),
            metric_allocations_getting_prepared.track_inprogress(),
            metric_allocation_preparation_latency.time(),
        ):
            metric_allocation_preparations.inc()
            alloc_info.input = await _prepare_alloc_input(
                alloc_info=alloc_info,
                blob_store=blob_store,
                logger=logger,
            )
            logger.info(
                "allocation was prepared for execution",
                duration=time.monotonic() - start_time,
            )
            return AllocationPreparationFinished(
                alloc_info=alloc_info,
                is_success=True,
            )
    except asyncio.CancelledError:
        return AllocationPreparationFinished(alloc_info=alloc_info, is_success=False)
    except BaseException as e:
        logger.error(
            "failed to prepare allocation for execution",
            exc_info=e,
            duration=time.monotonic() - start_time,
        )
        return AllocationPreparationFinished(alloc_info=alloc_info, is_success=False)


async def _prepare_alloc_input(
    alloc_info: AllocationInfo, blob_store: BLOBStore, logger: Any
) -> AllocationInput:
    """Prepares the alloc for execution.

    Raises an exception on error.
    """
    alloc: Allocation = alloc_info.allocation
    function_outputs_blob_uri: str = (
        f"{alloc.output_payload_uri_prefix}.{alloc_info.allocation.allocation_id}.output"
    )
    request_error_blob_uri: str = (
        f"{alloc.request_error_payload_uri_prefix}.{alloc.request_id}.req_error"
    )

    # The uploads are completed when finalizing the alloc.
    function_outputs_blob_upload_id: str | None = None
    request_error_blob_upload_id: str | None = None

    try:
        function_outputs_blob_upload_id = await blob_store.create_multipart_upload(
            uri=function_outputs_blob_uri,
            logger=logger,
        )
        request_error_blob_upload_id = await blob_store.create_multipart_upload(
            uri=request_error_blob_uri,
            logger=logger,
        )
    except BaseException:
        if function_outputs_blob_upload_id is not None:
            await blob_store.abort_multipart_upload(
                uri=function_outputs_blob_uri,
                upload_id=function_outputs_blob_upload_id,
                logger=logger,
            )
        if request_error_blob_upload_id is not None:
            await blob_store.abort_multipart_upload(
                uri=request_error_blob_uri,
                upload_id=request_error_blob_upload_id,
                logger=logger,
            )
        raise

    args: List[SerializedObjectInsideBLOB] = []
    arg_blobs: List[BLOB] = []
    for arg in alloc.args:
        arg: DataPayload
        args.append(_to_serialized_object_inside_blob(arg))
        arg_blob = await _presign_function_arg_blob(
            arg=arg,
            blob_store=blob_store,
            logger=logger,
        )
        arg_blobs.append(arg_blob)

    return AllocationInput(
        function_inputs=FunctionInputs(
            args=args,
            arg_blobs=arg_blobs,
            function_outputs_blob=await _presign_function_outputs_blob(
                uri=function_outputs_blob_uri,
                upload_id=function_outputs_blob_upload_id,
                blob_store=blob_store,
                logger=logger,
            ),
            request_error_blob=await _presign_request_error_blob(
                uri=request_error_blob_uri,
                upload_id=request_error_blob_upload_id,
                blob_store=blob_store,
                logger=logger,
            ),
            function_call_metadata=alloc.function_call_metadata,
        ),
        function_outputs_blob_uri=function_outputs_blob_uri,
        function_outputs_blob_upload_id=function_outputs_blob_upload_id,
        request_error_blob_uri=request_error_blob_uri,
        request_error_blob_upload_id=request_error_blob_upload_id,
    )


async def _presign_function_arg_blob(
    arg: DataPayload, blob_store: BLOBStore, logger: Any
) -> BLOB:
    get_blob_uri: str = await blob_store.presign_get_uri(
        uri=arg.uri,
        expires_in_sec=_MAX_PRESIGNED_URI_EXPIRATION_SEC,
        logger=logger,
    )
    chunks: List[BLOBChunk] = []

    while len(chunks) * _BLOB_OPTIMAL_CHUNK_SIZE_BYTES < arg.size:
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
    """Presigns the output blob for the allocation."""
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


async def _presign_request_error_blob(
    uri: str, upload_id: str, blob_store: BLOBStore, logger: Any
) -> BLOB:
    """Presigns the output blob for the request error."""
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
                size=_REQUEST_ERROR_MAX_SIZE_BYTES,
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
