from typing import Optional

from indexify.function_executor.proto.function_executor_pb2 import (
    FunctionOutput,
    RouterOutput,
)

from ..api_objects import Task


class TaskOutput:
    """Result of running a task."""

    def __init__(
        self,
        task: Task,
        function_output: Optional[FunctionOutput] = None,
        router_output: Optional[RouterOutput] = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        reducer: bool = False,
        success: bool = False,
    ):
        self.task = task
        self.function_output = function_output
        self.router_output = router_output
        self.stdout = stdout
        self.stderr = stderr
        self.reducer = reducer
        self.success = success

    @classmethod
    def internal_error(cls, task: Task) -> "TaskOutput":
        """Creates a TaskOutput for an internal error."""
        # We are not sharing internal error messages with the customer.
        return TaskOutput(task=task, stderr="Platform failed to execute the function.")
