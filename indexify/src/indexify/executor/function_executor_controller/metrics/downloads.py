import prometheus_client

from indexify.executor.monitoring.metrics import latency_metric_for_fast_operation

# Graph download metrics
metric_graph_downloads: prometheus_client.Counter = prometheus_client.Counter(
    "graph_downloads",
    "Number of graph downloads, including downloads served from local cache",
)
metric_graph_download_errors: prometheus_client.Counter = prometheus_client.Counter(
    "graph_download_errors",
    "Number of download errors, including downloads served from local cache",
)
metric_graphs_from_cache: prometheus_client.Counter = prometheus_client.Counter(
    "graph_downloads_from_cache",
    "Number of graph downloads served from local cache",
)
metric_graph_download_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "graph_download",
        "Graph download, including downloads served from local cache",
    )
)
