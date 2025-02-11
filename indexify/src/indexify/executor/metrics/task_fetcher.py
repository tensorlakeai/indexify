import prometheus_client

from ..monitoring.metrics import latency_metric_for_fast_operation

# This file contains all metrics used by TaskFetcher.

metric_server_registrations: prometheus_client.Counter = prometheus_client.Counter(
    "server_registration_requests",
    "Number of Executor registrations requests sent to the Server",
)
metric_server_registration_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "server_registration_request_errors",
        "Number of failed Executor registration requests",
    )
)
metric_server_registration_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "server_registration_request", "Register Executor at the Server"
    )
)
