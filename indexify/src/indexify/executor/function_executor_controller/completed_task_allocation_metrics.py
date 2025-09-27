import time
from typing import Any

from indexify.proto.executor_api_pb2 import (
    AllocationFailureReason,
    AllocationOutcomeCode,
)

from .metrics.completed_task_allocation_metrics import (
    METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALL,
    METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALLOCATION_CANCELLED,
    METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR,
    METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
    METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR,
    METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_NONE,
    METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_UNKNOWN,
    METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_ALL,
    METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
    METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_SUCCESS,
    metric_task_allocation_completion_latency,
    metric_task_allocations_completed,
)
from .task_allocation_info import TaskAllocationInfo


def emit_completed_task_allocation_metrics(
    alloc_info: TaskAllocationInfo, logger: Any
) -> None:
    """Emits Prometheus metrics for a completed task allocation.

    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)
    metric_task_allocation_completion_latency.observe(
        time.monotonic() - alloc_info.start_time
    )

    alloc_outcome_code: AllocationOutcomeCode = alloc_info.output.outcome_code
    alloc_failure_reason: AllocationFailureReason = alloc_info.output.failure_reason
    metric_task_allocations_completed.labels(
        outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_ALL,
        failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALL,
    ).inc()
    if alloc_outcome_code == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS:
        metric_task_allocations_completed.labels(
            outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_SUCCESS,
            failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_NONE,
        ).inc()
    elif alloc_outcome_code == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE:
        if (
            alloc_failure_reason
            == AllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR
        ):
            metric_task_allocations_completed.labels(
                outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR,
            ).inc()
        elif (
            alloc_failure_reason
            == AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED
        ):
            metric_task_allocations_completed.labels(
                outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
            ).inc()
        elif (
            alloc_failure_reason
            == AllocationFailureReason.ALLOCATION_FAILURE_REASON_ALLOCATION_CANCELLED
        ):
            metric_task_allocations_completed.labels(
                outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALLOCATION_CANCELLED,
            ).inc()
        elif alloc_failure_reason in [
            AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR,
            AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_TIMEOUT,
            AllocationFailureReason.ALLOCATION_FAILURE_REASON_REQUEST_ERROR,
        ]:
            metric_task_allocations_completed.labels(
                outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR,
            ).inc()
        else:
            metric_task_allocations_completed.labels(
                outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_UNKNOWN,
            ).inc()
            logger.warning(
                "unexpected task allocation failure reason",
                failure_reason=AllocationFailureReason.Name(alloc_failure_reason),
            )
