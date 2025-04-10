from typing import Dict, Optional

from tensorlake.function_executor.proto.function_executor_pb2 import (
    FunctionOutput,
    RouterOutput,
)


class TaskMetrics:
    """Metrics for a task."""

    def __init__(self, counters: Dict[str, int], timers: Dict[str, float]):
        self.counters = counters
        self.timers = timers


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
        output_payload_uri_prefix: Optional[str],
        output_encoding: Optional[str] = None,
        function_output: Optional[FunctionOutput] = None,
        router_output: Optional[RouterOutput] = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        reducer: bool = False,
        success: bool = False,
        is_internal_error: bool = False,
        metrics: Optional[TaskMetrics] = None,
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
        self.metrics = metrics
        self.output_encoding = output_encoding
        self.output_payload_uri_prefix = output_payload_uri_prefix

    @classmethod
    def internal_error(
        cls,
        task_id: str,
        namespace: str,
        graph_name: str,
        function_name: str,
        graph_version: str,
        graph_invocation_id: str,
        output_payload_uri_prefix: Optional[str],
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
            output_payload_uri_prefix=output_payload_uri_prefix,
        )

    @classmethod
    def function_timeout(
        cls,
        task_id: str,
        namespace: str,
        graph_name: str,
        function_name: str,
        graph_version: str,
        graph_invocation_id: str,
        timeout_sec: float,
        output_payload_uri_prefix: Optional[str],
    ) -> "TaskOutput":
        """Creates a TaskOutput for an function timeout error."""
        # Task stdout, stderr is not available.
        return TaskOutput(
            task_id=task_id,
            namespace=namespace,
            graph_name=graph_name,
            function_name=function_name,
            graph_version=graph_version,
            graph_invocation_id=graph_invocation_id,
            stderr=f"Function exceeded its configured timeout of {timeout_sec:.3f} sec.",
            is_internal_error=False,
            output_payload_uri_prefix=output_payload_uri_prefix,
        )
