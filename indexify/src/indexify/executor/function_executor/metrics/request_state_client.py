import prometheus_client

from ...monitoring.metrics import latency_metric_for_fast_operation

# This file contains all metrics used by RequestStateClient.

# General metrics.
metric_request_read_errors: prometheus_client.Counter = prometheus_client.Counter(
    "function_executor_request_state_client_request_read_errors",
    "Number of failed request reads in Function Executor Request State client resulting in its early termination",
)

# Get request state key-value Server API metrics.
metric_server_get_state_requests: prometheus_client.Counter = prometheus_client.Counter(
    "server_get_request_state_requests",
    "Number of get request state requests sent to the Server on behalf of Function Executor",
)
metric_server_get_state_request_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "server_get_request_state_request_errors",
        "Server get request state request errors",
    )
)
metric_server_get_state_request_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "server_get_request_state_request", "Server get request state request"
    )
)

# Set request state key-value Server API metrics.
metric_server_set_state_requests: prometheus_client.Counter = prometheus_client.Counter(
    "server_set_request_state_requests",
    "Number of set request state requests sent to the Server on behalf of Function Executor",
)
metric_server_set_state_request_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "server_set_request_state_request_errors",
        "Server set request state request errors",
    )
)
metric_server_set_state_request_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "server_set_request_state_request", "Server set request state request"
    )
)
