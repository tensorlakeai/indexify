import prometheus_client

from indexify.executor.monitoring.metrics import latency_metric_for_fast_operation

metric_task_preparations: prometheus_client.Counter = prometheus_client.Counter(
    "task_preparations", "Number of task preparations for execution"
)
metric_task_preparation_errors: prometheus_client.Counter = prometheus_client.Counter(
    "task_preparation_errors", "Number of task preparation errors"
)
metric_task_preparation_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "task_preparation", "task preparation for execution"
    )
)
metric_tasks_getting_prepared: prometheus_client.Gauge = prometheus_client.Gauge(
    "tasks_getting_prepared", "Number of tasks currently getting prepared for execution"
)
