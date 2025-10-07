import prometheus_client

from indexify.executor.monitoring.metrics import (
    latency_metric_for_customer_controlled_operation,
)

metric_allocations_completed: prometheus_client.Counter = prometheus_client.Counter(
    "allocations_completed",
    "Number of allocations that were completed",
    ["outcome_code", "failure_reason"],
)
METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_ALL = "all"
METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_SUCCESS = "success"
METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE = "failure"

METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALL = "all"
# Used when the allocation is successfull.
METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_NONE = "none"
# Matches ALLOCATION_FAILURE_REASON_UNKNOWN
METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_UNKNOWN = "unknown"
# Includes all function errors including timeouts to reduce cardinality.
METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR = "function_error"
# Includes all internal errors to reduce cardinality.
METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR = "internal_error"
# Matches ALLOCATION_FAILURE_REASON_ALLOCATION_CANCELLED
METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALLOCATION_CANCELLED = "alloc_cancelled"
# Matches ALLOCATION_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED
METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED = (
    "function_executor_terminated"
)

# Valid combinations of the labels:
metric_allocations_completed.labels(
    outcome_code=METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_ALL,
    failure_reason=METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALL,
)
metric_allocations_completed.labels(
    outcome_code=METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_SUCCESS,
    failure_reason=METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_NONE,
)

metric_allocations_completed.labels(
    outcome_code=METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_UNKNOWN,
)
metric_allocations_completed.labels(
    outcome_code=METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR,
)
metric_allocations_completed.labels(
    outcome_code=METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR,
)
metric_allocations_completed.labels(
    outcome_code=METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_ALLOCATION_CANCELLED,
)
metric_allocations_completed.labels(
    outcome_code=METRIC_ALLOCATIONS_COMPLETED_OUTCOME_CODE_FAILURE,
    failure_reason=METRIC_ALLOCATIONS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
)

metric_allocation_completion_latency: prometheus_client.Histogram = (
    latency_metric_for_customer_controlled_operation(
        "allocation_completion",
        "allocation completion from the moment it got fetched until its output got uploaded to blob store",
    )
)
