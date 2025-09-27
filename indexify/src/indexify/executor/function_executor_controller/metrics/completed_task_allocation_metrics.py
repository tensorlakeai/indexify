import prometheus_client

from indexify.executor.monitoring.metrics import (
    latency_metric_for_customer_controlled_operation,
)

metric_task_allocations_completed: prometheus_client.Counter = (
    prometheus_client.Counter(
        "task_allocations_completed",
        "Number of task allocations that were completed",
        ["outcome_code", "failure_reason"],
    )
)
METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_ALL = "all"
METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_SUCCESS = "success"
METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE = "failure"

METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALL = "all"
# Used when the task allocation is successfull.
METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_NONE = "none"
# Matches ALLOCATION_FAILURE_REASON_UNKNOWN
METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_UNKNOWN = "unknown"
# Includes all function errors including timeouts to reduce cardinality.
METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR = "function_error"
# Includes all internal errors to reduce cardinality.
METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR = "internal_error"
# Matches ALLOCATION_FAILURE_REASON_ALLOCATION_CANCELLED
METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALLOCATION_CANCELLED = (
    "alloc_cancelled"
)
# Matches ALLOCATION_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED
METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED = (
    "function_executor_terminated"
)

# Valid combinations of the labels:
metric_task_allocations_completed.labels(
    outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_ALL,
    failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALL,
)
metric_task_allocations_completed.labels(
    outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_SUCCESS,
    failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_NONE,
)

metric_task_allocations_completed.labels(
    outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_UNKNOWN,
)
metric_task_allocations_completed.labels(
    outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR,
)
metric_task_allocations_completed.labels(
    outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR,
)
metric_task_allocations_completed.labels(
    outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALLOCATION_CANCELLED,
)
metric_task_allocations_completed.labels(
    outcome_code=METRIC_TASK_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_TASK_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
)

metric_task_allocation_completion_latency: prometheus_client.Histogram = (
    latency_metric_for_customer_controlled_operation(
        "task_allocation_completion",
        "task allocation completion from the moment it got fetched until its output got uploaded to blob store",
    )
)
