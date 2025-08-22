import time
from typing import Any

from indexify.proto.executor_api_pb2 import (
    TaskFailureReason,
    TaskOutcomeCode,
)

from .metrics.completed_task_allocation_metrics import (
    METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALL,
    METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR,
    METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
    METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR,
    METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_NONE,
    METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_TASK_CANCELLED,
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

    task_outcome_code: TaskOutcomeCode = alloc_info.output.outcome_code
    task_failure_reason: TaskFailureReason = alloc_info.output.failure_reason
    metric_task_allocations_completed.labels(
        outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_ALL,
        failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALL,
    ).inc()
    if task_outcome_code == TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS:
        metric_task_allocations_completed.labels(
            outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_SUCCESS,
            failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_NONE,
        ).inc()
    elif task_outcome_code == TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE:
        if task_failure_reason == TaskFailureReason.TASK_FAILURE_REASON_INTERNAL_ERROR:
            metric_task_allocations_completed.labels(
                outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR,
            ).inc()
        elif (
            task_failure_reason
            == TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED
        ):
            metric_task_allocations_completed.labels(
                outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
            ).inc()
        elif (
            task_failure_reason == TaskFailureReason.TASK_FAILURE_REASON_TASK_CANCELLED
        ):
            metric_task_allocations_completed.labels(
                outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_TASK_CANCELLED,
            ).inc()
        elif task_failure_reason in [
            TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_ERROR,
            TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_TIMEOUT,
            TaskFailureReason.TASK_FAILURE_REASON_INVOCATION_ERROR,
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
                failure_reason=TaskFailureReason.Name(task_failure_reason),
            )
