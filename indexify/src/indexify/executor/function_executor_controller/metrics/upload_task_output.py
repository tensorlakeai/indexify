import prometheus_client

from indexify.executor.monitoring.metrics import latency_metric_for_fast_operation

# Task output upload metrics.
metric_task_output_uploads: prometheus_client.Counter = prometheus_client.Counter(
    "task_output_uploads",
    "Number of task output uploads",
)
metric_tasks_uploading_outputs: prometheus_client.Gauge = prometheus_client.Gauge(
    "tasks_uploading_output",
    "Number of tasks currently uploading their outputs",
)
metric_task_output_upload_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation("task_output_upload", "task output upload")
)
metric_task_output_upload_retries: prometheus_client.Counter = (
    prometheus_client.Counter(
        "tasks_output_upload_retries", "Number of task output upload retries"
    )
)

# Metrics for individual blob store operations.
metric_task_output_blob_store_uploads: prometheus_client.Counter = (
    prometheus_client.Counter(
        "task_output_blob_store_uploads", "Number of task output uploads to blob store"
    )
)
metric_task_output_blob_store_upload_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "task_output_blob_store_upload_errors",
        "Number of failed task output uploads to blob store",
    )
)
metric_task_output_blob_store_upload_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "task_output_blob_store_upload", "Upload task output to blob store"
    )
)
