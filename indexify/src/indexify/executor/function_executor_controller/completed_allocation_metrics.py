import time
from typing import Any

from indexify.proto.executor_api_pb2 import (
    AllocationFailureReason,
    AllocationOutcomeCode,
)

from .allocation_info import AllocationInfo
from .metrics.completed_allocation_metrics import (
    METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALL,
    METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALLOCATION_CANCELLED,
    METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR,
    METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
    METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR,
    METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_NONE,
    METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_UNKNOWN,
    METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_ALL,
    METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
    METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_SUCCESS,
    metric_allocation_completion_latency,
    metric_allocations_completed,
)


def emit_completed_allocation_metrics(alloc_info: AllocationInfo, logger: Any) -> None:
    """Emits Prometheus metrics for a completed allocation.

    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)
    metric_allocation_completion_latency.observe(
        time.monotonic() - alloc_info.start_time
    )

    alloc_outcome_code: AllocationOutcomeCode = alloc_info.output.outcome_code
    alloc_failure_reason: AllocationFailureReason = alloc_info.output.failure_reason
    metric_allocations_completed.labels(
        outcome_code=METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_ALL,
        failure_reason=METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALL,
    ).inc()
    if alloc_outcome_code == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS:
        metric_allocations_completed.labels(
            outcome_code=METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_SUCCESS,
            failure_reason=METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_NONE,
        ).inc()
    elif alloc_outcome_code == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE:
        if (
            alloc_failure_reason
            == AllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR
        ):
            metric_allocations_completed.labels(
                outcome_code=METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR,
            ).inc()
        elif (
            alloc_failure_reason
            == AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED
        ):
            metric_allocations_completed.labels(
                outcome_code=METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
            ).inc()
        elif (
            alloc_failure_reason
            == AllocationFailureReason.ALLOCATION_FAILURE_REASON_ALLOCATION_CANCELLED
        ):
            metric_allocations_completed.labels(
                outcome_code=METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALLOCATION_CANCELLED,
            ).inc()
        elif alloc_failure_reason in [
            AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR,
            AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_TIMEOUT,
            AllocationFailureReason.ALLOCATION_FAILURE_REASON_REQUEST_ERROR,
        ]:
            metric_allocations_completed.labels(
                outcome_code=METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR,
            ).inc()
        else:
            metric_allocations_completed.labels(
                outcome_code=METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_UNKNOWN,
            ).inc()
            logger.warning(
                "unexpected allocation failure reason",
                failure_reason=AllocationFailureReason.Name(alloc_failure_reason),
            )
