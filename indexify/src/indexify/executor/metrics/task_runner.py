import prometheus_client

from ..monitoring.metrics import latency_metric_for_customer_controlled_operation

# This file contains all metrics used by TaskRunner.

# Metrics for the stage when task is blocked by the current policy.
metric_task_policy_runs: prometheus_client.Counter = prometheus_client.Counter(
    "task_policy_runs",
    "Number of task execution policy runs",
)
metric_task_policy_errors: prometheus_client.Counter = prometheus_client.Counter(
    "task_policy_errors",
    "Number of errors while running task execution policy",
)
metric_task_policy_latency: prometheus_client.Histogram = (
    latency_metric_for_customer_controlled_operation(
        "task_policy",
        "Task execution blocked by the policy",
    )
)
metric_tasks_blocked_by_policy: prometheus_client.Gauge = prometheus_client.Gauge(
    "tasks_blocked_by_policy",
    "Number of tasks that are ready for execution but are blocked according to the current policy (typically waiting for a free Function Executor)",
)
metric_tasks_blocked_by_policy_per_function_name: prometheus_client.Gauge = (
    prometheus_client.Gauge(
        "tasks_blocked_by_policy_per_function_name",
        "Number of tasks that are ready for execution but are blocked according to the current policy (typically waiting for a free Function Executor)",
        ["function_name"],
    )
)

# Metrics for the stage when task is running.
metric_task_runs: prometheus_client.Counter = prometheus_client.Counter(
    "task_runs",
    "Number of task runs",
)
metric_task_run_platform_errors: prometheus_client.Counter = prometheus_client.Counter(
    "task_run_platform_errors",
    "Number of platform errors while running task",
)
metric_task_run_latency: prometheus_client.Histogram = (
    latency_metric_for_customer_controlled_operation(
        "task_run",
        "run task from the moment it is unblocked by the policy until it finishes",
    )
)
metric_tasks_running: prometheus_client.Gauge = prometheus_client.Gauge(
    "tasks_running",
    "Number of running tasks",
)
