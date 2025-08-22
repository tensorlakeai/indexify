import prometheus_client

from indexify.executor.monitoring.metrics import (
    latency_metric_for_customer_controlled_operation,
)

metric_function_executor_run_task_rpcs: prometheus_client.Counter = (
    prometheus_client.Counter(
        "function_executor_run_task_rpcs",
        "Number of Function Executor run task lifecycle RPC sequences",
    )
)
metric_function_executor_run_task_rpc_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "function_executor_run_task_rpc_errors",
        "Number of Function Executor run task lifecycle RPC errors",
    )
)
metric_function_executor_run_task_rpc_latency: prometheus_client.Histogram = (
    latency_metric_for_customer_controlled_operation(
        "function_executor_run_task_rpc", "Function Executor run task lifecycle RPC"
    )
)
metric_function_executor_run_task_rpcs_in_progress: prometheus_client.Gauge = (
    prometheus_client.Gauge(
        "function_executor_run_task_rpcs_in_progress",
        "Number of Function Executor run task lifecycle RPCs in progress",
    )
)
