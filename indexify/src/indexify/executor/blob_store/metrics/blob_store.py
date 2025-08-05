import prometheus_client

from ...monitoring.metrics import latency_metric_for_fast_operation

metric_get_blob_requests: prometheus_client.Counter = prometheus_client.Counter(
    "blob_store_get_requests",
    "Number of get BLOB requests in BLOB store",
)
metric_get_blob_errors: prometheus_client.Counter = prometheus_client.Counter(
    "blob_store_get_request_errors",
    "Number of get BLOB request errors in BLOB store",
)
metric_get_blob_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "blob_store_get",
        "BLOB store get BLOB request",
    )
)

metric_presign_uri_requests: prometheus_client.Counter = prometheus_client.Counter(
    "blob_store_presign_uri_requests",
    "Number of presign URI requests in BLOB store",
)
metric_presign_uri_errors: prometheus_client.Counter = prometheus_client.Counter(
    "blob_store_presign_uri_request_errors",
    "Number of presign URI request errors in BLOB store",
)
metric_presign_uri_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "blob_store_presign_uri",
        "BLOB store presign URI request",
    )
)

metric_upload_blob_requests: prometheus_client.Counter = prometheus_client.Counter(
    "blob_store_upload_requests",
    "Number of upload BLOB requests in BLOB store",
)
metric_upload_blob_errors: prometheus_client.Counter = prometheus_client.Counter(
    "blob_store_upload_request_errors",
    "Number of upload BLOB request errors in BLOB store",
)
metric_upload_blob_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "blob_store_upload",
        "BLOB store upload BLOB request",
    )
)

metric_create_multipart_upload_requests: prometheus_client.Counter = (
    prometheus_client.Counter(
        "blob_store_create_multipart_upload_requests",
        "Number of create multipart upload requests in BLOB store",
    )
)
metric_create_multipart_upload_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "blob_store_create_multipart_upload_request_errors",
        "Number of create multipart upload request errors in BLOB store",
    )
)
metric_create_multipart_upload_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "blob_store_create_multipart_upload_request",
        "create multipart upload request in BLOB store",
    )
)

metric_complete_multipart_upload_requests: prometheus_client.Counter = (
    prometheus_client.Counter(
        "blob_store_complete_multipart_upload_requests",
        "Number of complete multipart upload requests in BLOB store",
    )
)
metric_complete_multipart_upload_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "blob_store_complete_multipart_upload_request_errors",
        "Number of complete multipart upload request errors in BLOB store",
    )
)
metric_complete_multipart_upload_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "blob_store_complete_multipart_upload_request",
        "complete multipart upload request in BLOB store",
    )
)

metric_abort_multipart_upload_requests: prometheus_client.Counter = (
    prometheus_client.Counter(
        "blob_store_abort_multipart_upload_requests",
        "Number of abort multipart upload requests in BLOB store",
    )
)
metric_abort_multipart_upload_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "blob_store_abort_multipart_upload_request_errors",
        "Number of abort multipart upload request errors in BLOB store",
    )
)
metric_abort_multipart_upload_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "blob_store_abort_multipart_upload_request",
        "abort multipart upload request in BLOB store",
    )
)
