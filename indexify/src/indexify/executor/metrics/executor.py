import prometheus_client

from ..monitoring.metrics import (
    latency_metric_for_customer_controlled_operation,
    latency_metric_for_fast_operation,
)

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
