import prometheus_client

metric_healthy = prometheus_client.Gauge(
    "healthy", "1 if the executor is healthy, 0 otherwise"
)
