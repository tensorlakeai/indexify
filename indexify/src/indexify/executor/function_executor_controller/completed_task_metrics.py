import time
from typing import Any

from indexify.proto.executor_api_pb2 import (
    TaskFailureReason,
    TaskOutcomeCode,
)

from .metrics.completed_task_metrics import (
    METRIC_TASKS_COMPLETED_FAILURE_REASON_ALL,
    METRIC_TASKS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR,
    METRIC_TASKS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
    METRIC_TASKS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR,
    METRIC_TASKS_COMPLETED_FAILURE_REASON_NONE,
    METRIC_TASKS_COMPLETED_FAILURE_REASON_TASK_CANCELLED,
    METRIC_TASKS_COMPLETED_FAILURE_REASON_UNKNOWN,
    METRIC_TASKS_COMPLETED_OUTCOME_CODE_ALL,
    METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
    METRIC_TASKS_COMPLETED_OUTCOME_CODE_SUCCESS,
    metric_task_completion_latency,
    metric_tasks_completed,
)
from .task_info import TaskInfo


def emit_completed_task_metrics(task_info: TaskInfo, logger: Any) -> None:
    """Emits Prometheus metrics for a completed task.

    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)
    metric_task_completion_latency.observe(time.monotonic() - task_info.start_time)

    task_outcome_code: TaskOutcomeCode = task_info.output.outcome_code
    task_failure_reason: TaskFailureReason = task_info.output.failure_reason
    metric_tasks_completed.labels(
        outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_ALL,
        failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_ALL,
    ).inc()
    if task_outcome_code == TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS:
        metric_tasks_completed.labels(
            outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_SUCCESS,
            failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_NONE,
        ).inc()
    elif task_outcome_code == TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE:
        if task_failure_reason == TaskFailureReason.TASK_FAILURE_REASON_INTERNAL_ERROR:
            metric_tasks_completed.labels(
                outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR,
            ).inc()
        elif (
            task_failure_reason
            == TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED
        ):
            metric_tasks_completed.labels(
                outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
            ).inc()
        elif (
            task_failure_reason == TaskFailureReason.TASK_FAILURE_REASON_TASK_CANCELLED
        ):
            metric_tasks_completed.labels(
                outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_TASK_CANCELLED,
            ).inc()
        elif task_failure_reason in [
            TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_ERROR,
            TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_TIMEOUT,
            TaskFailureReason.TASK_FAILURE_REASON_INVOCATION_ERROR,
        ]:
            metric_tasks_completed.labels(
                outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR,
            ).inc()
        else:
            metric_tasks_completed.labels(
                outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_UNKNOWN,
            ).inc()
            logger.warning(
                "unexpected task failure reason",
                failure_reason=TaskFailureReason.Name(task_failure_reason),
            )
