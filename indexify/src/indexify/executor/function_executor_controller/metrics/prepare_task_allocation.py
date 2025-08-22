import prometheus_client

from indexify.executor.monitoring.metrics import latency_metric_for_fast_operation

metric_task_allocation_preparations: prometheus_client.Counter = (
    prometheus_client.Counter(
        "task_allocation_preparations",
        "Number of task allocation preparations for execution",
    )
)
metric_task_allocation_preparation_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "task_allocation_preparation_errors",
        "Number of task allocation preparation errors",
    )
)
metric_task_allocation_preparation_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "task_allocation_preparation", "task allocation preparation for execution"
    )
)
metric_task_allocations_getting_prepared: prometheus_client.Gauge = (
    prometheus_client.Gauge(
        "task_allocations_getting_prepared",
        "Number of task allocations currently getting prepared for execution",
    )
)
