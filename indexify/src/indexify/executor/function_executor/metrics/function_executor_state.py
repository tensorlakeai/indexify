import prometheus_client

# This file contains all metrics used by FunctionExecutorState.

metric_function_executor_state_not_locked_errors: prometheus_client.Counter = (
    prometheus_client.Counter(
        "function_executor_state_not_locked_errors",
        "Number of times a Function Executor state was used without acquiring its lock",
    )
)
