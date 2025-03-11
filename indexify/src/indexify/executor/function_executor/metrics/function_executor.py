import prometheus_client

from ...monitoring.metrics import (
    latency_metric_for_customer_controlled_operation,
    latency_metric_for_fast_operation,
    latency_metric_for_slow_operation,
)

# This file contains all metrics used by FunctionExecutor.

metric_function_executors_count = prometheus_client.Gauge(
    "function_executors_count", "Number of existing Function Executors"
)

# Metrics about whole FE creation workflow.
metric_creations: prometheus_client.Counter = prometheus_client.Counter(
    "function_executor_creates",
    "Number of Function Executor creations",
)
metric_create_latency: prometheus_client.Histogram = (
    latency_metric_for_customer_controlled_operation(
        "function_executor_create", "Function Executor creation (aka cold start)"
    )
)
metric_create_errors: prometheus_client.Counter = prometheus_client.Counter(
    "function_executor_create_errors", "Number of Function Executor creation errors"
)

# Metrics about whole FE destroy workflow.
metric_destroys: prometheus_client.Counter = prometheus_client.Counter(
    "function_executor_destroys", "Number of Function Executor destructions"
)
metric_destroy_latency: prometheus_client.Histogram = latency_metric_for_slow_operation(
    "function_executor_destroy", "Function Executor destruction"
)
metric_destroy_errors: prometheus_client.Counter = prometheus_client.Counter(
    "function_executor_destroy_errors",
    "Number of Function Executor destruction errors, results in a resource leak",
)

# FE server create and destruction metrics.
metric_create_server_latency: prometheus_client.Histogram = (
    latency_metric_for_slow_operation(
        "function_executor_create_server", "Function Executor server creation"
    )
)
metric_create_server_errors: prometheus_client.Counter = prometheus_client.Counter(
    "function_executor_create_server_errors",
    "Number of Function Executor server creation errors",
)
metric_destroy_server_latency: prometheus_client.Histogram = (
    latency_metric_for_slow_operation(
        "function_executor_destroy_server", "Function Executor server destruction"
    )
)
metric_destroy_server_errors: prometheus_client.Counter = prometheus_client.Counter(
    "function_executor_destroy_server_errors",
    "Number of Function Executor server destruction errors",
)

# FE channel creation and destruction metrics.
metric_establish_channel_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "function_executor_establish_channel", "Function Executor channel establishment"
    )
)
metric_establish_channel_errors: prometheus_client.Counter = prometheus_client.Counter(
    "function_executor_establish_channel_errors",
    "Number of Function Executor channel establishment errors",
)
metric_destroy_channel_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "function_executor_destroy_channel", "Function Executor channel destruction"
    )
)
metric_destroy_channel_errors: prometheus_client.Counter = prometheus_client.Counter(
    "function_executor_destroy_channel_errors",
    "Number of Function Executor channel destruction errors",
)

# FE get_info RPC metrics.
metric_get_info_rpc_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "function_executor_get_info_rpc", "Function Executor get_info RPC"
    )
)
metric_get_info_rpc_errors: prometheus_client.Counter = prometheus_client.Counter(
    "function_executor_get_info_rpc_errors",
    "Number of Function Executor get_info RPC errors",
)
metric_function_executor_infos: prometheus_client.Counter = prometheus_client.Counter(
    "function_executor_infos",
    "Number of Function Executor creations with particular info",
    ["version", "sdk_version", "sdk_language", "sdk_language_version"],
)

# FE initialization RPC metrics.
metric_initialize_rpc_latency: prometheus_client.Histogram = (
    latency_metric_for_customer_controlled_operation(
        "function_executor_initialize_rpc", "Function Executor initialize RPC"
    )
)
metric_initialize_rpc_errors: prometheus_client.Counter = prometheus_client.Counter(
    "function_executor_initialize_rpc_errors",
    "Number of Function Executor initialize RPC errors",
)

# FE invocation state client creation and destruction metrics.
metric_create_invocation_state_client_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "function_executor_create_invocation_state_client",
        "Function Executor invocation state client creation",
    )
)
metric_create_invocation_state_client_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "function_executor_create_invocation_state_client_errors",
        "Number of Function Executor invocation state client creation errors",
    )
)
metric_destroy_invocation_state_client_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "function_executor_destroy_invocation_state_client",
        "Function Executor invocation state client destruction",
    )
)
metric_destroy_invocation_state_client_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "function_executor_destroy_invocation_state_client_errors",
        "Number of Function Executor invocation state client destruction errors",
    )
)

# FE health checker creation and destruction metrics.
metric_create_health_checker_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "function_executor_create_health_checker",
        "Function Executor health checker creation",
    )
)
metric_create_health_checker_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "function_executor_create_health_checker_errors",
        "Number of Function Executor health checker creation errors",
    )
)
metric_destroy_health_checker_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "function_executor_destroy_health_checker",
        "Function Executor health checker destruction",
    )
)
metric_destroy_health_checker_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "function_executor_destroy_health_checker_errors",
        "Number of Function Executor health checker destruction errors",
    )
)
