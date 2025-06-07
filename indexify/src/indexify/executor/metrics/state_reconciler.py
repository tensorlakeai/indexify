import prometheus_client

from ..monitoring.metrics import latency_metric_for_fast_operation

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
