import prometheus_client

from ...monitoring.metrics import latency_metric_for_customer_controlled_operation

# This file contains all metrics used by SingleTaskRunner.

metric_function_executor_run_task_rpcs: prometheus_client.Counter = (
    prometheus_client.Counter(
        "function_executor_run_task_rpcs", "Number of Function Executor run task RPCs"
    )
)
metric_function_executor_run_task_rpc_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "function_executor_run_task_rpc_errors",
        "Number of Function Executor run task RPC errors",
    )
)
metric_function_executor_run_task_rpc_latency: prometheus_client.Histogram = (
    latency_metric_for_customer_controlled_operation(
        "function_executor_run_task_rpc", "Function Executor run task RPC"
    )
)
