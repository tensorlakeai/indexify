import asyncio
import time
from typing import Any

from indexify.executor.blob_store.blob_store import BLOBStore

from .allocation_info import AllocationInfo
from .allocation_input import AllocationInput
from .allocation_output import AllocationOutput
from .events import AllocationFinalizationFinished
from .metrics.finalize_allocation import (
    metric_allocation_finalization_errors,
    metric_allocation_finalization_latency,
    metric_allocation_finalizations,
    metric_allocations_finalizing,
)


async def finalize_allocation(
    alloc_info: AllocationInfo, blob_store: BLOBStore, logger: Any
) -> AllocationFinalizationFinished:
    """Does post-processing of allocation output, cleans up its resources.

    Doesn't raise any Exceptions.
    """
    logger = logger.bind(module=__name__)
    start_time = time.monotonic()

    with (
        metric_allocations_finalizing.track_inprogress(),
        metric_allocation_finalization_latency.time(),
        metric_allocation_finalization_errors.count_exceptions(),
    ):
        metric_allocation_finalizations.inc()
        try:
            await _finalize_alloc_output(
                alloc_info=alloc_info,
                blob_store=blob_store,
                logger=logger,
            )
            logger.info(
                "allocation finalized",
                duration=time.monotonic() - start_time,
            )
            return AllocationFinalizationFinished(
                alloc_info=alloc_info, is_success=True
            )
        except asyncio.CancelledError:
            return AllocationFinalizationFinished(
                alloc_info=alloc_info, is_success=False
            )
        except BaseException as e:
            logger.error(
                "failed to finalize allocation",
                exc_info=e,
                duration=time.monotonic() - start_time,
            )
            return AllocationFinalizationFinished(
                alloc_info=alloc_info, is_success=False
            )


async def _finalize_alloc_output(
    alloc_info: AllocationInfo, blob_store: BLOBStore, logger: Any
) -> None:
    """Finalizes the allocation output.

    Raises exception on error."""
    if alloc_info.input is None:
        raise Exception(
            "allocation input is None, this should never happen",
        )
    if alloc_info.output is None:
        raise Exception(
            "allocation output is None, this should never happen",
        )

    input: AllocationInput = alloc_info.input
    output: AllocationOutput = alloc_info.output

    if output.fe_result is not None and output.fe_result.HasField(
        "request_error_output"
    ):
        await blob_store.complete_multipart_upload(
            uri=input.request_error_blob_uri,
            upload_id=input.request_error_blob_upload_id,
            parts_etags=[
                blob_chunk.etag
                for blob_chunk in output.fe_result.uploaded_request_error_blob.chunks
            ],
            logger=logger,
        )
    else:
        await blob_store.abort_multipart_upload(
            uri=input.request_error_blob_uri,
            upload_id=input.request_error_blob_upload_id,
            logger=logger,
        )
