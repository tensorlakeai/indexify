import prometheus_client

# This file contains all metrics used by FunctionExecutorStatesContainer.

metric_function_executor_states_count: prometheus_client.Gauge = (
    prometheus_client.Gauge(
        "function_executor_states_count",
        "Number of existing Function Executor states",
    )
)
