from tensorlake.function_executor.proto.function_executor_pb2 import (
    ExecutionPlanUpdates as FEExecutionPlanUpdates,
)

from indexify.proto.executor_api_pb2 import (
    ExecutionPlanUpdates as ServerExecutionPlanUpdates,
)


def from_fe_execution_plan_updates(
    fe_execution_plan_updates: FEExecutionPlanUpdates,
) -> ServerExecutionPlanUpdates:
    """Converts from Function Executor's ExecutionPlanUpdates to Server ExecutionPlanUpdates."""
    pass
