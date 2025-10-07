import prometheus_client

from indexify.executor.monitoring.metrics import latency_metric_for_fast_operation

# Allocation finalization metrics.
metric_allocation_finalizations: prometheus_client.Counter = prometheus_client.Counter(
    "allocation_finalizations",
    "Number of allocation finalizations",
)
metric_allocation_finalization_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "allocation_finalization_errors",
        "Number of allocation finalization errors",
    )
)
metric_allocations_finalizing: prometheus_client.Gauge = prometheus_client.Gauge(
    "allocations_finalizing",
    "Number of allocations currently finalizing",
)
metric_allocation_finalization_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "allocation_finalization", "allocation finalization"
    )
)
