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

metric_tasks_fetched: prometheus_client.Counter = prometheus_client.Counter(
    "tasks_fetched", "Number of tasks that were fetched from Server"
)

metric_schedule_task_latency: prometheus_client.Histogram = (
    latency_metric_for_customer_controlled_operation(
        "schedule_task",
        "Schedule a task for execution after it got ready for execution",
    )
)
metric_runnable_tasks: prometheus_client.Gauge = prometheus_client.Gauge(
    "runnable_tasks",
    "Number of tasks that are ready for execution but are waiting to get scheduled to run on Function Executor (typically waiting for a free Function Executor)",
)
metric_runnable_tasks_per_function_name: prometheus_client.Gauge = (
    prometheus_client.Gauge(
        "runnable_tasks_per_function_name",
        "Number of tasks that are ready for execution but are waiting to get scheduled to run on Function Executor (typically waiting for a free Function Executor)",
        ["function_name"],
    )
)

metric_function_executors_with_status: prometheus_client.Gauge = (
    prometheus_client.Gauge(
        "function_executors_with_status",
        "Number of Function Executors with a particular status",
        ["status"],
    )
)
METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_UNKNOWN = "unknown"
METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_PENDING = "pending"
METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_RUNNING = "running"
METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_TERMINATED = "terminated"

metric_function_executors_with_status.labels(
    status=METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_UNKNOWN
)
metric_function_executors_with_status.labels(
    status=METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_PENDING
)
metric_function_executors_with_status.labels(
    status=METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_RUNNING
)
metric_function_executors_with_status.labels(
    status=METRIC_FUNCTION_EXECUTORS_WITH_STATUS_LABEL_TERMINATED
)
