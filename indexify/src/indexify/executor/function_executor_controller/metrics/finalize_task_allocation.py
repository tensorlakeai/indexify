import prometheus_client

from indexify.executor.monitoring.metrics import latency_metric_for_fast_operation

# Task allocation finalization metrics.
metric_task_allocation_finalizations: prometheus_client.Counter = (
    prometheus_client.Counter(
        "task_allocation_finalizations",
        "Number of task allocation finalizations",
    )
)
metric_task_allocation_finalization_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "task_allocation_finalization_errors",
        "Number of task allocation finalization errors",
    )
)
metric_task_allocations_finalizing: prometheus_client.Gauge = prometheus_client.Gauge(
    "task_allocations_finalizing",
    "Number of task allocations currently finalizing",
)
metric_task_allocation_finalization_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "task_allocation_finalization", "task allocation finalization"
    )
)
