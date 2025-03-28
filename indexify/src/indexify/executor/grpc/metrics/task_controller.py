import prometheus_client

from ...monitoring.metrics import latency_metric_for_fast_operation

metric_task_cancellations = prometheus_client.Counter(
    "task_cancellations",
    "Number of times a task was cancelled",
)
