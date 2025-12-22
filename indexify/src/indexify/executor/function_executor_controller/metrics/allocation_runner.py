import prometheus_client

from indexify.executor.monitoring.metrics import (
    latency_metric_for_customer_controlled_operation,
    latency_metric_for_fast_operation,
)

metric_allocation_runner_allocation_runs: prometheus_client.Counter = (
    prometheus_client.Counter(
        "allocation_runs",
        "Number of allocation runs on Function Executor",
    )
)

metric_allocation_runner_allocation_run_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "allocation_run_errors",
        "Number of allocation run errors on Function Executor",
    )
)


metric_allocation_runner_allocation_runs_in_progress: prometheus_client.Gauge = (
    prometheus_client.Gauge(
        "allocation_runs_in_progress",
        "Number of allocation runs currently in progress on Function Executor",
    )
)


# This metric provides a basic observability into customer code execution duration.
# This allows to rule out simple cases of elevated latencies being caused by customer code.
metric_allocation_runner_allocation_run_latency: prometheus_client.Histogram = (
    latency_metric_for_customer_controlled_operation(
        "allocation_run",
        "Run allocation on Function Executor",
    )
)

# Server call_function RPC metrics.
metric_call_function_rpcs = prometheus_client.Counter(
    "call_function_rpcs",
    "Number of Executor call function RPCs to Server",
)
metric_call_function_rpc_errors = prometheus_client.Counter(
    "call_function_rpc_errors",
    "Number of Executor to Server call function RPC errors",
)
metric_call_function_rpc_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "call_function_rpc", "Executor call function rpc to Server"
    )
)

metric_function_call_message_size_mb = prometheus_client.Histogram(
    "function_call_message_size_mb",
    "Size of function call request message sent from Executor to Server in MB",
    # We cap message size at less than 100 MB but adding more buckets in case we go over it by mistake.
    buckets=[
        0.01,
        0.1,
        0.5,
        1.0,
        5.0,
        10.0,
        20.0,
        30.0,
        40.0,
        50.0,
        60.0,
        70.0,
        80.0,
        90.0,
        100.0,
        150.0,
        200.0,
        1000.0,
        10000.0,
    ],
)
