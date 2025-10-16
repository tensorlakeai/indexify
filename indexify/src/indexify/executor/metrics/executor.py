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

metric_executor_events_pushed: prometheus_client.Counter = prometheus_client.Counter(
    "executor_events_pushed", "Number of events pushed to collector"
)

metric_executor_event_push_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "executor_event_push_errors",
        "Number of errors while pushing events to collector",
    )
)
