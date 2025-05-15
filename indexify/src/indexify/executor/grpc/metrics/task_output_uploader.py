import prometheus_client

from ...monitoring.metrics import latency_metric_for_fast_operation

# This file contains all metrics used by TaskReporter.

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
