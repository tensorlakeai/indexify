import prometheus_client

from indexify.executor.monitoring.metrics import (
    latency_metric_for_customer_controlled_operation,
    latency_metric_for_fast_operation,
)

metric_control_loop_handle_event_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "handle_function_executor_control_loop_event",
        "Handle Function Executor control loop event",
    )
)

metric_allocations_fetched: prometheus_client.Counter = prometheus_client.Counter(
    "allocations_fetched",
    "Number of allocations that were fetched from Server",
)

metric_schedule_allocation_latency: prometheus_client.Histogram = (
    latency_metric_for_customer_controlled_operation(
        "schedule_allocation",
        "Schedule an allocation for execution after it got ready for execution",
    )
)
metric_runnable_allocations: prometheus_client.Gauge = prometheus_client.Gauge(
    "runnable_allocations",
    "Number of allocations that are ready for execution but are waiting to get scheduled to run on Function Executor (typically waiting for a free Function Executor)",
)
metric_runnable_allocations_per_function_name: prometheus_client.Gauge = (
    prometheus_client.Gauge(
        "runnable_allocations_per_function_name",
        "Number of allocations that are ready for execution but are waiting to get scheduled to run on Function Executor (typically waiting for a free Function Executor)",
        ["function_name"],
    )
)

metric_function_executors_with_state: prometheus_client.Gauge = prometheus_client.Gauge(
    "function_executors_with_state",
    "Number of Function Executors with a particular internal state",
    ["state"],
)
METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_UNKNOWN = "unknown"
METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_NOT_STARTED = "not_started"
METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_STARTING_UP = "starting_up"
METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_RUNNING = "running"
METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_TERMINATING = "terminating"
METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_TERMINATED = "terminated"


metric_function_executors_with_state.labels(
    state=METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_UNKNOWN
)
metric_function_executors_with_state.labels(
    state=METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_NOT_STARTED
)
metric_function_executors_with_state.labels(
    state=METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_STARTING_UP
)
metric_function_executors_with_state.labels(
    state=METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_RUNNING
)
metric_function_executors_with_state.labels(
    state=METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_TERMINATING
)
metric_function_executors_with_state.labels(
    state=METRIC_FUNCTION_EXECUTORS_WITH_STATE_LABEL_TERMINATED
)
