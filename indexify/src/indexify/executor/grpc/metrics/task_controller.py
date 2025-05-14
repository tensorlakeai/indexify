import prometheus_client

from ...monitoring.metrics import (
    latency_metric_for_customer_controlled_operation,
    latency_metric_for_fast_operation,
)

metric_task_cancellations = prometheus_client.Counter(
    "task_cancellations",
    "Number of times a task was cancelled",
)

# Task statistics metrics.
metric_tasks_fetched: prometheus_client.Counter = prometheus_client.Counter(
    "tasks_fetched", "Number of tasks that were fetched from Server"
)
metric_tasks_completed: prometheus_client.Counter = prometheus_client.Counter(
    "tasks_completed", "Number of tasks that were completed", ["outcome"]
)
METRIC_TASKS_COMPLETED_OUTCOME_ALL = "all"
METRIC_TASKS_COMPLETED_OUTCOME_SUCCESS = "success"
METRIC_TASKS_COMPLETED_OUTCOME_ERROR_CUSTOMER_CODE = "error_customer_code"
METRIC_TASKS_COMPLETED_OUTCOME_ERROR_PLATFORM = "error_platform"
metric_tasks_completed.labels(outcome=METRIC_TASKS_COMPLETED_OUTCOME_ALL)
metric_tasks_completed.labels(outcome=METRIC_TASKS_COMPLETED_OUTCOME_SUCCESS)
metric_tasks_completed.labels(
    outcome=METRIC_TASKS_COMPLETED_OUTCOME_ERROR_CUSTOMER_CODE
)
metric_tasks_completed.labels(outcome=METRIC_TASKS_COMPLETED_OUTCOME_ERROR_PLATFORM)
metric_task_completion_latency: prometheus_client.Histogram = (
    latency_metric_for_customer_controlled_operation(
        "task_completion",
        "task completion from the moment it got fetched until its outcome got reported",
    )
)

# Task outcome reporting metrics.
metric_task_outcome_reports: prometheus_client.Counter = prometheus_client.Counter(
    "task_outcome_reports",
    "Number of task outcome reports",
)
metric_tasks_reporting_outcome: prometheus_client.Gauge = prometheus_client.Gauge(
    "tasks_reporting_outcome",
    "Number of tasks currently reporting their outcomes",
)
metric_task_outcome_report_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation("task_outcome_report", "task outcome report")
)
metric_task_outcome_report_retries: prometheus_client.Counter = (
    prometheus_client.Counter(
        "tasks_outcome_report_retries", "Number of task outcome report retries"
    )
)

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
