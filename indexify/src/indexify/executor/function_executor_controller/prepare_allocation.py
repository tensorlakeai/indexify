import asyncio
import time
from typing import Any, List

from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    FunctionInputs,
    SerializedObjectInsideBLOB,
)

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.proto.executor_api_pb2 import Allocation, DataPayload

from .allocation_info import AllocationInfo
from .allocation_input import AllocationInput
from .blob_utils import (
    data_payload_to_serialized_object_inside_blob,
    presign_read_only_blob_for_data_payload,
    presign_write_only_blob,
)
from .events import AllocationPreparationFinished
from .metrics.prepare_allocation import (
    metric_allocation_preparation_errors,
    metric_allocation_preparation_latency,
    metric_allocation_preparations,
    metric_allocations_getting_prepared,
)

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
    request_error_blob_uri: str = (
        f"{alloc.request_error_payload_uri_prefix}.{alloc.request_id}.req_error"
    )

    # The upload is completed when finalizing the alloc.
    request_error_blob_upload_id: str = await blob_store.create_multipart_upload(
        uri=request_error_blob_uri,
        logger=logger,
    )

    try:
        request_error_blob: BLOB = await presign_write_only_blob(
            blob_id="request_error_blob",
            blob_uri=request_error_blob_uri,
            upload_id=request_error_blob_upload_id,
            size=_REQUEST_ERROR_MAX_SIZE_BYTES,
            blob_store=blob_store,
            logger=logger,
        )
    except BaseException:
        # Clean up the multipart upload on error.
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
        args.append(data_payload_to_serialized_object_inside_blob(arg))
        arg_blob = await presign_read_only_blob_for_data_payload(
            data_payload=arg,
            blob_store=blob_store,
            logger=logger,
        )
        arg_blobs.append(arg_blob)

    return AllocationInput(
        function_inputs=FunctionInputs(
            args=args,
            arg_blobs=arg_blobs,
            request_error_blob=request_error_blob,
            function_call_metadata=alloc.function_call_metadata,
        ),
        request_error_blob_uri=request_error_blob_uri,
        request_error_blob_upload_id=request_error_blob_upload_id,
    )
