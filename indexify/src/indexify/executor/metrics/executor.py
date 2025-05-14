import prometheus_client

# This file contains all metrics used by Executor.

# Executor overview metrics.
metric_executor_info: prometheus_client.Info = prometheus_client.Info(
    "executor", "Executor information"
)
metric_executor_state: prometheus_client.Enum = prometheus_client.Enum(
    "executor_state",
    "Current Executor state",
    states=["starting", "running", "shutting_down"],
)
