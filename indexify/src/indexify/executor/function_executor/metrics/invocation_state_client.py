import prometheus_client

from ...monitoring.metrics import latency_metric_for_fast_operation

# This file contains all metrics used by InvocationStateClient.

# General metrics.
metric_request_read_errors: prometheus_client.Counter = prometheus_client.Counter(
    "function_executor_invocation_state_client_request_read_errors",
    "Number of failed request reads in Function Executor Invocation State client resulting in its early termination",
)

# Get invocation state key-value Server API metrics.
metric_server_get_state_requests: prometheus_client.Counter = prometheus_client.Counter(
    "server_get_invocation_state_requests",
    "Number of get invocation state requests sent to the Server on behalf of Function Executor",
)
metric_server_get_state_request_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "server_get_invocation_state_request_errors",
        "Server get invocation state request errors",
    )
)
metric_server_get_state_request_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "server_get_invocation_state_request", "Server get invocation state request"
    )
)

# Set invocation state key-value Server API metrics.
metric_server_set_state_requests: prometheus_client.Counter = prometheus_client.Counter(
    "server_set_invocation_state_requests",
    "Number of set invocation state requests sent to the Server on behalf of Function Executor",
)
metric_server_set_state_request_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "server_set_invocation_state_request_errors",
        "Server set invocation state request errors",
    )
)
metric_server_set_state_request_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "server_set_invocation_state_request", "Server set invocation state request"
    )
)
