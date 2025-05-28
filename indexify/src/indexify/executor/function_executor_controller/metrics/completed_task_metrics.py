import prometheus_client

from indexify.executor.monitoring.metrics import (
    latency_metric_for_customer_controlled_operation,
)

metric_tasks_completed: prometheus_client.Counter = prometheus_client.Counter(
    "tasks_completed",
    "Number of tasks that were completed",
    ["outcome_code", "failure_reason"],
)
METRIC_TASKS_COMPLETED_OUTCOME_CODE_ALL = "all"
METRIC_TASKS_COMPLETED_OUTCOME_CODE_SUCCESS = "success"
METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE = "failure"

METRIC_TASKS_COMPLETED_FAILURE_REASON_ALL = "all"
# Used when the task is successfull.
METRIC_TASKS_COMPLETED_FAILURE_REASON_NONE = "none"
# Matches TASK_FAILURE_REASON_UNKNOWN
METRIC_TASKS_COMPLETED_FAILURE_REASON_UNKNOWN = "unknown"
# Includes all function errors including timeouts to reduce cardinality.
METRIC_TASKS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR = "function_error"
# Includes all internal errors to reduce cardinality.
METRIC_TASKS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR = "internal_error"
# Matches TASK_FAILURE_REASON_TASK_CANCELLED
METRIC_TASKS_COMPLETED_FAILURE_REASON_TASK_CANCELLED = "task_cancelled"
# Matches TASK_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED
METRIC_TASKS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED = (
    "function_executor_terminated"
)

# Valid combinations of the labels:
metric_tasks_completed.labels(
    outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_ALL,
    failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_ALL,
)
metric_tasks_completed.labels(
    outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_SUCCESS,
    failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_NONE,
)

metric_tasks_completed.labels(
    outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_UNKNOWN,
)
metric_tasks_completed.labels(
    outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR,
)
metric_tasks_completed.labels(
    outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR,
)
metric_tasks_completed.labels(
    outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_TASK_CANCELLED,
)
metric_tasks_completed.labels(
    outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
)

metric_task_completion_latency: prometheus_client.Histogram = (
    latency_metric_for_customer_controlled_operation(
        "task_completion",
        "task completion from the moment it got fetched until its output got uploaded to blob store",
    )
)
