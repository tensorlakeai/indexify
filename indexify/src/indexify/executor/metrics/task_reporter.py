import prometheus_client

from ..monitoring.metrics import latency_metric_for_fast_operation

# This file contains all metrics used by TaskReporter.

metric_server_ingest_files_requests: prometheus_client.Counter = (
    prometheus_client.Counter(
        "server_ingest_files_requests", "Number of Server ingest files requests"
    )
)
metric_server_ingest_files_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "server_ingest_files_request_errors",
        "Number of Server ingest files request errors",
    )
)
metric_server_ingest_files_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "server_ingest_files_request", "Ingest files request to Server"
    )
)

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

metric_report_task_outcome_rpcs = prometheus_client.Counter(
    "report_task_outcome_rpcs",
    "Number of report task outcome RPCs to Server",
)
metric_report_task_outcome_errors = prometheus_client.Counter(
    "report_task_outcome_rpc_errors",
    "Number of report task outcome RPC errors",
)
metric_report_task_outcome_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "report_task_outcome_rpc", "Report task outcome RPC to Server"
    )
)
