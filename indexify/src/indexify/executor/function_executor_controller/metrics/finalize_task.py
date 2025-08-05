import prometheus_client

from indexify.executor.monitoring.metrics import latency_metric_for_fast_operation

# Task finalization metrics.
metric_task_finalizations: prometheus_client.Counter = prometheus_client.Counter(
    "task_finalizations",
    "Number of task finalizations",
)
metric_task_finalization_errors: prometheus_client.Counter = prometheus_client.Counter(
    "task_finalization_errors",
    "Number of task finalization errors",
)
metric_tasks_finalizing: prometheus_client.Gauge = prometheus_client.Gauge(
    "tasks_finalizing",
    "Number of tasks currently finalizing",
)
metric_task_finalization_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation("task_finalization", "task finalization")
)
