import prometheus_client

from indexify.executor.monitoring.metrics import (
    latency_metric_for_customer_controlled_operation,
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
