import prometheus_client

from ...monitoring.metrics import latency_metric_for_fast_operation

# This file contains all metrics used by HealthChecker.

metric_failed_health_checks = prometheus_client.Counter(
    "function_executor_failed_health_checks",
    "Number of health checks that were not successful",
)
metric_health_check_latency = latency_metric_for_fast_operation(
    "function_executor_health_check",
    "Function Executor health check",
)
