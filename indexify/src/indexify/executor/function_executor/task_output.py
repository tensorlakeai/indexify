from typing import Optional

from tensorlake.function_executor.proto.function_executor_pb2 import (
    FunctionOutput,
    RouterOutput,
)

from ..api_objects import Task


class TaskOutput:
    """Result of running a task."""

    def __init__(
        self,
        task_id: str,
        namespace: str,
        graph_name: str,
        function_name: str,
        graph_version: str,
        graph_invocation_id: str,
        function_output: Optional[FunctionOutput] = None,
        router_output: Optional[RouterOutput] = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        reducer: bool = False,
        success: bool = False,
        is_internal_error: bool = False,
    ):
        self.task_id = task_id
        self.namespace = namespace
        self.graph_name = graph_name
        self.function_name = function_name
        self.graph_version = graph_version
        self.graph_invocation_id = graph_invocation_id
        self.function_output = function_output
        self.router_output = router_output
        self.stdout = stdout
        self.stderr = stderr
        self.reducer = reducer
        self.success = success
        self.is_internal_error = is_internal_error

    @classmethod
    def internal_error(
        cls,
        task_id: str,
        namespace: str,
        graph_name: str,
        function_name: str,
        graph_version: str,
        graph_invocation_id: str,
    ) -> "TaskOutput":
        """Creates a TaskOutput for an internal error."""
        # We are not sharing internal error messages with the customer.
        return TaskOutput(
            task_id=task_id,
            namespace=namespace,
            graph_name=graph_name,
            function_name=function_name,
            graph_version=graph_version,
            graph_invocation_id=graph_invocation_id,
            stderr="Platform failed to execute the function.",
            is_internal_error=True,
        )
