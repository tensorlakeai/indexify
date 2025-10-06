import prometheus_client

from indexify.executor.monitoring.metrics import latency_metric_for_fast_operation

metric_allocation_preparations: prometheus_client.Counter = prometheus_client.Counter(
    "allocation_preparations",
    "Number of allocation preparations for execution",
)
metric_allocation_preparation_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "allocation_preparation_errors",
        "Number of allocation preparation errors",
    )
)
metric_allocation_preparation_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "allocation_preparation", "allocation preparation for execution"
    )
)
metric_allocations_getting_prepared: prometheus_client.Gauge = prometheus_client.Gauge(
    "allocations_getting_prepared",
    "Number of allocations currently getting prepared for execution",
)
