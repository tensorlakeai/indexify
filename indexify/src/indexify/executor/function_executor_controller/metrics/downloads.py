import prometheus_client

from indexify.executor.monitoring.metrics import latency_metric_for_fast_operation

# Application download metrics
metric_application_downloads: prometheus_client.Counter = prometheus_client.Counter(
    "application_downloads",
    "Number of application downloads, including downloads served from local cache",
)
metric_application_download_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "application_download_errors",
        "Number of application download errors, including downloads served from local cache",
    )
)
metric_applications_from_cache: prometheus_client.Counter = prometheus_client.Counter(
    "application_downloads_from_cache",
    "Number of application downloads served from local cache",
)
metric_application_download_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "application_download",
        "Application download, including downloads served from local cache",
    )
)
