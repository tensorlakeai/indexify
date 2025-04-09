import prometheus_client

from ...monitoring.metrics import latency_metric_for_fast_operation

metric_get_blob_requests: prometheus_client.Counter = prometheus_client.Counter(
    "get_blob_requests",
    "Number of get blob requests",
)
metric_get_blob_errors: prometheus_client.Counter = prometheus_client.Counter(
    "get_blob_request_errors",
    "Number of get blob request errors",
)
metric_get_blob_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "get_blob_request",
        "get blob request",
    )
)

metric_put_blob_requests: prometheus_client.Counter = prometheus_client.Counter(
    "put_blob_requests",
    "Number of put blob requests",
)
metric_put_blob_errors: prometheus_client.Counter = prometheus_client.Counter(
    "put_blob_request_errors",
    "Number of put blob request errors",
)
metric_put_blob_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "put_blob_request",
        "put blob request",
    )
)
