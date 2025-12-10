import prometheus_client

from ..monitoring.metrics import latency_metric_for_fast_operation

metric_desired_state_streams = prometheus_client.Counter(
    "desired_state_streams",
    "Number of desired states streams created",
)
metric_desired_state_stream_errors = prometheus_client.Counter(
    "desired_state_stream_errors",
    "Number of desired state stream errors",
)

metric_state_reconciliations = prometheus_client.Counter(
    "state_reconciliations",
    "Number of Executor state reconciliations",
)
metric_state_reconciliation_errors = prometheus_client.Counter(
    "state_reconciliation_errors",
    "Number of Executor state reconciliation errors after all retries",
)
metric_state_reconciliation_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "state_reconciliation", "Executor state reconciliation"
    )
)

metric_last_desired_state_allocations = prometheus_client.Gauge(
    "last_desired_state_allocations",
    "Number of allocations in the last desired state recieved from Server",
)
metric_last_desired_state_function_executors = prometheus_client.Gauge(
    "last_desired_state_function_executors",
    "Number of function executors in the last desired state recieved from Server",
)
