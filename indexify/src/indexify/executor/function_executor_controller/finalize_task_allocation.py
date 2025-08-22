import asyncio
import time
from typing import Any

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.proto.executor_api_pb2 import (
    TaskFailureReason,
    TaskOutcomeCode,
)

from .events import TaskAllocationFinalizationFinished
from .metrics.finalize_task_allocation import (
    metric_task_allocation_finalization_errors,
    metric_task_allocation_finalization_latency,
    metric_task_allocation_finalizations,
    metric_task_allocations_finalizing,
)
from .task_allocation_info import TaskAllocationInfo
from .task_allocation_input import TaskAllocationInput
from .task_allocation_output import TaskAllocationOutput


async def finalize_task_allocation(
    task_alloc: TaskAllocationInfo, blob_store: BLOBStore, logger: Any
) -> TaskAllocationFinalizationFinished:
    """Prepares the task output for getting it reported to Server.

    The task output is either coming from a failed task or from its finished execution on the Function Executor.
    Doesn't raise any Exceptions.
    """
    logger = logger.bind(module=__name__)
    start_time = time.monotonic()

    with (
        metric_task_allocations_finalizing.track_inprogress(),
        metric_task_allocation_finalization_latency.time(),
        metric_task_allocation_finalization_errors.count_exceptions(),
    ):
        metric_task_allocation_finalizations.inc()
        try:
            await _finalize_task_alloc_output(
                alloc_info=task_alloc,
                blob_store=blob_store,
                logger=logger,
            )
            logger.info(
                "task allocation finalized",
                duration=time.monotonic() - start_time,
            )
            return TaskAllocationFinalizationFinished(
                alloc_info=task_alloc, is_success=True
            )
        except asyncio.CancelledError:
            return TaskAllocationFinalizationFinished(
                alloc_info=task_alloc, is_success=False
            )
        except BaseException as e:
            logger.error(
                "failed to finalize task allocation",
                exc_info=e,
                duration=time.monotonic() - start_time,
            )
            return TaskAllocationFinalizationFinished(
                alloc_info=task_alloc, is_success=False
            )


class _TaskAllocationOutputSummary:
    def __init__(self):
        self.output_count: int = 0
        self.output_bytes: int = 0
        self.invocation_error_output_count: int = 0
        self.invocation_error_output_bytes: int = 0
        self.next_functions_count: int = 0


async def _finalize_task_alloc_output(
    alloc_info: TaskAllocationInfo, blob_store: BLOBStore, logger: Any
) -> None:
    """Finalizes the task output.

    Raises exception on error."""
    if alloc_info.input is None:
        raise Exception(
            "task allocation input is None, this should never happen",
        )
    if alloc_info.output is None:
        raise Exception(
            "task allocation output is None, this should never happen",
        )

    input: TaskAllocationInput = alloc_info.input
    output: TaskAllocationOutput = alloc_info.output

    output_summary: _TaskAllocationOutputSummary = _task_output_summary(output)
    logger.info(
        "task allocation output summary",
        output_count=output_summary.output_count,
        output_bytes=output_summary.output_bytes,
        invocation_error_output_count=output_summary.invocation_error_output_count,
        invocation_error_output_bytes=output_summary.invocation_error_output_bytes,
        next_functions_count=output_summary.next_functions_count,
    )

    _log_function_metrics(output, logger)

    if output.outcome_code == TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS:
        if len(output.uploaded_function_outputs_blob.chunks) == 0:
            # No output from function, usually means it returns None.
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
            uri=input.invocation_error_blob_uri,
            upload_id=input.invocation_error_blob_upload_id,
            logger=logger,
        )
    elif output.outcome_code == TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE:
        await blob_store.abort_multipart_upload(
            uri=input.function_outputs_blob_uri,
            upload_id=input.function_outputs_blob_upload_id,
            logger=logger,
        )
        if (
            output.failure_reason
            == TaskFailureReason.TASK_FAILURE_REASON_INVOCATION_ERROR
        ) and len(output.uploaded_invocation_error_blob.chunks) != 0:
            await blob_store.complete_multipart_upload(
                uri=input.invocation_error_blob_uri,
                upload_id=input.invocation_error_blob_upload_id,
                parts_etags=[
                    blob_chunk.etag
                    for blob_chunk in output.uploaded_invocation_error_blob.chunks
                ],
                logger=logger,
            )
        else:
            await blob_store.abort_multipart_upload(
                uri=input.invocation_error_blob_uri,
                upload_id=input.invocation_error_blob_upload_id,
                logger=logger,
            )
    else:
        raise ValueError(
            f"Unexpected outcome code: {TaskOutcomeCode.Name(output.outcome_code)}"
        )


def _task_output_summary(
    task_output: TaskAllocationOutput,
) -> _TaskAllocationOutputSummary:
    summary: _TaskAllocationOutputSummary = _TaskAllocationOutputSummary()

    for output in task_output.function_outputs:
        summary.output_count += 1
        summary.output_bytes += output.manifest.size

    if task_output.invocation_error_output is not None:
        summary.invocation_error_output_count = 1
        summary.invocation_error_output_bytes = (
            task_output.invocation_error_output.manifest.size
        )

    summary.next_functions_count = len(task_output.next_functions)

    return summary


# Temporary workaround is logging customer metrics until we store them somewhere
# for future retrieval and processing.
def _log_function_metrics(output: TaskAllocationOutput, logger: Any):
    if output.metrics is None:
        return

    for counter_name, counter_value in output.metrics.counters.items():
        logger.info(
            "function_metric", counter_name=counter_name, counter_value=counter_value
        )
    for timer_name, timer_value in output.metrics.timers.items():
        logger.info("function_metric", timer_name=timer_name, timer_value=timer_value)
