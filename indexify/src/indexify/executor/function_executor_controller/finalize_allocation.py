import asyncio
import time
from typing import Any

from tensorlake.function_executor.proto.function_executor_pb2 import (
    ExecutionPlanUpdate,
    FunctionArg,
    FunctionCall,
    ReduceOp,
)

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.proto.executor_api_pb2 import (
    AllocationFailureReason,
    AllocationOutcomeCode,
)

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
    """Prepares the alloc output for getting it reported to Server.

    The alloc output is either coming from a failed alloc or from its finished execution on the Function Executor.
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


class _AllocationOutputSummary:
    def __init__(self):
        self.values_count: int = 0
        self.values_bytes: int = 0
        self.function_calls_count: int = 0
        self.reducer_calls_count: int = 0
        self.is_request_error: bool = False
        self.request_error_bytes: int = 0


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

    output_summary: _AllocationOutputSummary = _alloc_output_summary(output)
    logger.info(
        "allocation output summary",
        values_count=output_summary.values_count,
        values_bytes=output_summary.values_bytes,
        function_calls_count=output_summary.function_calls_count,
        reducer_calls_count=output_summary.reducer_calls_count,
        is_request_error=output_summary.is_request_error,
        request_error_bytes=output_summary.request_error_bytes,
    )

    _log_function_metrics(output, logger)

    if output.outcome_code == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS:
        if len(output.uploaded_function_outputs_blob.chunks) == 0:
            # A function always returns at least None so this branch should never be taken
            # unless FE misbehaved. No outputs were uploaded by function so
            # complete_multipart_upload will fail. So we abort the upload instead.
            await blob_store.abort_multipart_upload(
                uri=input.function_outputs_blob_uri,
                upload_id=input.function_outputs_blob_upload_id,
                logger=logger,
            )
        else:
            await blob_store.complete_multipart_upload(
                uri=input.function_outputs_blob_uri,
                upload_id=input.function_outputs_blob_upload_id,
                parts_etags=[
                    blob_chunk.etag
                    for blob_chunk in output.uploaded_function_outputs_blob.chunks
                ],
                logger=logger,
            )
        await blob_store.abort_multipart_upload(
            uri=input.request_error_blob_uri,
            upload_id=input.request_error_blob_upload_id,
            logger=logger,
        )
    elif output.outcome_code == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE:
        await blob_store.abort_multipart_upload(
            uri=input.function_outputs_blob_uri,
            upload_id=input.function_outputs_blob_upload_id,
            logger=logger,
        )
        if (
            output.failure_reason
            == AllocationFailureReason.ALLOCATION_FAILURE_REASON_REQUEST_ERROR
        ):
            if len(output.uploaded_request_error_blob.chunks) == 0:
                # A function that fails with request error always returns at least one request error output
                # so this branch should never be taken unless FE misbehaved. No request error outputs were
                # uploaded by function so complete_multipart_upload will fail. So we abort the upload instead.
                await blob_store.abort_multipart_upload(
                    uri=input.request_error_blob_uri,
                    upload_id=input.request_error_blob_upload_id,
                    logger=logger,
                )
            else:
                await blob_store.complete_multipart_upload(
                    uri=input.request_error_blob_uri,
                    upload_id=input.request_error_blob_upload_id,
                    parts_etags=[
                        blob_chunk.etag
                        for blob_chunk in output.uploaded_request_error_blob.chunks
                    ],
                    logger=logger,
                )
        else:
            await blob_store.abort_multipart_upload(
                uri=input.request_error_blob_uri,
                upload_id=input.request_error_blob_upload_id,
                logger=logger,
            )
    else:
        raise ValueError(
            f"Unexpected alloction outcome code: {AllocationOutcomeCode.Name(output.outcome_code)}"
        )


def _alloc_output_summary(
    alloc_output: AllocationOutput,
) -> _AllocationOutputSummary:
    # self.values_count: int = 0
    # self.values_bytes: int = 0
    # self.function_calls_count: int = 0
    # self.reducer_calls_count: int = 0
    summary: _AllocationOutputSummary = _AllocationOutputSummary()

    if alloc_output.output_value is not None:
        summary.values_count += 1
        summary.values_bytes += alloc_output.output_value.manifest.size

    if alloc_output.output_execution_plan_updates is not None:
        for update in alloc_output.output_execution_plan_updates.updates:
            update: ExecutionPlanUpdate
            if update.HasField("function_call"):
                summary.function_calls_count += 1
                func_call: FunctionCall = update.function_call
                for arg in func_call.args:
                    arg: FunctionArg
                    if arg.HasField("value"):
                        summary.values_count += 1
                        summary.values_bytes += arg.value.manifest.size
            elif update.HasField("reduce"):
                summary.reducer_calls_count += 1
                reduce_op: ReduceOp = update.reduce
                for arg in reduce_op.collection:
                    arg: FunctionArg
                    if arg.HasField("value"):
                        summary.values_count += 1
                        summary.values_bytes += arg.value.manifest.size

    if alloc_output.request_error_output is not None:
        summary.is_request_error = True
        summary.request_error_bytes = alloc_output.request_error_output.manifest.size

    return summary


# Temporary workaround is logging customer metrics until we store them somewhere
# for future retrieval and processing.
def _log_function_metrics(output: AllocationOutput, logger: Any):
    if output.metrics is None:
        return

    for counter_name, counter_value in output.metrics.counters.items():
        logger.info(
            "function_metric", counter_name=counter_name, counter_value=counter_value
        )
    for timer_name, timer_value in output.metrics.timers.items():
        logger.info("function_metric", timer_name=timer_name, timer_value=timer_value)
