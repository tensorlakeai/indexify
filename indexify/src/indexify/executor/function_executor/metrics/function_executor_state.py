import prometheus_client

from ..function_executor_status import FunctionExecutorStatus

# This file contains all metrics used by FunctionExecutorState.

metric_function_executor_state_not_locked_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "function_executor_state_not_locked_errors",
        "Number of times a Function Executor state was used without acquiring its lock",
    )
)

# Function Executors count with a particular status.
metric_function_executors_with_status: prometheus_client.Gauge = (
    prometheus_client.Gauge(
        "function_executors_with_status",
        "Number of Function Executors with a particular status",
        ["status"],
    )
)
metric_function_executors_with_status.labels(
    status=FunctionExecutorStatus.STARTING_UP.name
)
metric_function_executors_with_status.labels(
    status=FunctionExecutorStatus.STARTUP_FAILED_CUSTOMER_ERROR.name
)
metric_function_executors_with_status.labels(
    status=FunctionExecutorStatus.STARTUP_FAILED_PLATFORM_ERROR.name
)
metric_function_executors_with_status.labels(status=FunctionExecutorStatus.IDLE.name)
metric_function_executors_with_status.labels(
    status=FunctionExecutorStatus.RUNNING_TASK.name
)
metric_function_executors_with_status.labels(
    status=FunctionExecutorStatus.UNHEALTHY.name
)
metric_function_executors_with_status.labels(
    status=FunctionExecutorStatus.DESTROYING.name
)
metric_function_executors_with_status.labels(
    status=FunctionExecutorStatus.DESTROYED.name
)
metric_function_executors_with_status.labels(
    status=FunctionExecutorStatus.SHUTDOWN.name
)
