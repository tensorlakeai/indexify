import prometheus_client

from indexify.executor.monitoring.metrics import latency_metric_for_fast_operation

# Graph download metrics
metric_graph_downloads: prometheus_client.Counter = prometheus_client.Counter(
    "task_graph_downloads",
    "Number of task graph downloads, including downloads served from local cache",
)
metric_graph_download_errors: prometheus_client.Counter = prometheus_client.Counter(
    "task_graph_download_errors",
    "Number of task download errors, including downloads served from local cache",
)
metric_graphs_from_cache: prometheus_client.Counter = prometheus_client.Counter(
    "task_graph_downloads_from_cache",
    "Number of task graph downloads served from local cache",
)
metric_graph_download_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "task_graph_download",
        "task graph download, including downloads served from local cache",
    )
)
metric_tasks_downloading_graphs: prometheus_client.Gauge = prometheus_client.Gauge(
    "tasks_downloading_graphs",
    "Number of tasks currently downloading their graphs, including local cache lookups",
)

# Task input download metrics
metric_task_input_downloads: prometheus_client.Counter = prometheus_client.Counter(
    "task_input_downloads", "Number of task input downloads"
)
metric_task_input_download_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "task_input_download_errors", "Number of task input download errors"
    )
)
metric_task_input_download_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation("task_input_download", "task input download")
)
metric_tasks_downloading_inputs: prometheus_client.Gauge = prometheus_client.Gauge(
    "tasks_downloading_inputs", "Number of tasks currently downloading their inputs"
)

# Reducer init value download metrics
metric_reducer_init_value_downloads: prometheus_client.Counter = (
    prometheus_client.Counter(
        "task_reducer_init_value_downloads", "Number of reducer init value downloads"
    )
)
metric_reducer_init_value_download_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "task_reducer_init_value_download_errors",
        "Number of reducer init value download errors",
    )
)
metric_reducer_init_value_download_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "task_reducer_init_value_download", "Task reducer init value download"
    )
)
metric_tasks_downloading_reducer_init_value: prometheus_client.Gauge = (
    prometheus_client.Gauge(
        "tasks_downloading_reducer_init_value",
        "Number of tasks currently downloading their reducer init values",
    )
)
