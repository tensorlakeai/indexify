from typing import Any

from indexify.proto.executor_api_pb2 import (
    FunctionExecutorDescription,
    Task,
)


def function_executor_logger(
    function_executor_description: FunctionExecutorDescription, logger: Any
) -> Any:
    """Returns a logger bound with the FE's metadata.

    The function assumes that the FE might be invalid."""
    return logger.bind(
        function_executor_id=(
            function_executor_description.id
            if function_executor_description.HasField("id")
            else None
        ),
        namespace=(
            function_executor_description.namespace
            if function_executor_description.HasField("namespace")
            else None
        ),
        graph_name=(
            function_executor_description.graph_name
            if function_executor_description.HasField("graph_name")
            else None
        ),
        graph_version=(
            function_executor_description.graph_version
            if function_executor_description.HasField("graph_version")
            else None
        ),
        function_name=(
            function_executor_description.function_name
            if function_executor_description.HasField("function_name")
            else None
        ),
    )


def task_logger(task: Task, logger: Any) -> Any:
    """Returns a logger bound with the task's metadata.

    The function assumes that the task might be invalid."""
    return logger.bind(
        task_id=task.id if task.HasField("id") else None,
        namespace=task.namespace if task.HasField("namespace") else None,
        graph_name=task.graph_name if task.HasField("graph_name") else None,
        graph_version=task.graph_version if task.HasField("graph_version") else None,
        function_name=task.function_name if task.HasField("function_name") else None,
        graph_invocation_id=(
            task.graph_invocation_id if task.HasField("graph_invocation_id") else None
        ),
    )
