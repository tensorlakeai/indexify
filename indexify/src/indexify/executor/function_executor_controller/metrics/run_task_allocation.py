import prometheus_client

from indexify.executor.monitoring.metrics import (
    latency_metric_for_customer_controlled_operation,
)

metric_function_executor_run_allocation_rpcs: prometheus_client.Counter = (
    prometheus_client.Counter(
        "function_executor_run_allocation_rpcs",
        "Number of Function Executor run allocation lifecycle RPC sequences",
    )
)
metric_function_executor_run_allocation_rpc_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "function_executor_run_allocation_rpc_errors",
        "Number of Function Executor run allocation lifecycle RPC errors",
    )
)
metric_function_executor_run_allocation_rpc_latency: prometheus_client.Histogram = (
    latency_metric_for_customer_controlled_operation(
        "function_executor_run_allocation_rpc",
        "Function Executor run allocation lifecycle RPC",
    )
)
metric_function_executor_run_allocation_rpcs_in_progress: prometheus_client.Gauge = (
    prometheus_client.Gauge(
        "function_executor_run_allocation_rpcs_in_progress",
        "Number of Function Executor run allocation lifecycle RPCs in progress",
    )
)
