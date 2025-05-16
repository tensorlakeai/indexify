from typing import Dict, List, Optional

from tensorlake.function_executor.proto.function_executor_pb2 import (
    FunctionOutput,
    RouterOutput,
)

from indexify.proto.executor_api_pb2 import (
    DataPayload,
    TaskFailureReason,
    TaskOutcomeCode,
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
        output_payload_uri_prefix: str,
        outcome_code: TaskOutcomeCode,
        # Optional[TaskFailureReason] is not supported in python 3.9
        failure_reason: TaskFailureReason = None,
        output_encoding: Optional[str] = None,
        function_output: Optional[FunctionOutput] = None,
        router_output: Optional[RouterOutput] = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        reducer: bool = False,
        metrics: Optional[TaskMetrics] = None,
        uploaded_data_payloads: List[DataPayload] = [],
        uploaded_stdout: Optional[DataPayload] = None,
        uploaded_stderr: Optional[DataPayload] = None,
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
        self.outcome_code = outcome_code
        self.failure_reason = failure_reason
        self.metrics = metrics
        self.output_encoding = output_encoding
        self.output_payload_uri_prefix = output_payload_uri_prefix
        self.uploaded_data_payloads = uploaded_data_payloads
        self.uploaded_stdout = uploaded_stdout
        self.uploaded_stderr = uploaded_stderr

    @classmethod
    def internal_error(
        cls,
        task_id: str,
        namespace: str,
        graph_name: str,
        function_name: str,
        graph_version: str,
        graph_invocation_id: str,
        output_payload_uri_prefix: str,
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
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_INTERNAL_ERROR,
            stderr="Platform failed to execute the function.",
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
        output_payload_uri_prefix: str,
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
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_TIMEOUT,
            stderr=f"Function or router exceeded its configured timeout of {timeout_sec:.3f} sec.",
            output_payload_uri_prefix=output_payload_uri_prefix,
        )

    @classmethod
    def task_cancelled(
        cls,
        task_id: str,
        namespace: str,
        graph_name: str,
        function_name: str,
        graph_version: str,
        graph_invocation_id: str,
        output_payload_uri_prefix: str,
    ) -> "TaskOutput":
        """Creates a TaskOutput for the case when task didn't finish because its allocation was removed by Server."""
        return TaskOutput(
            task_id=task_id,
            namespace=namespace,
            graph_name=graph_name,
            function_name=function_name,
            graph_version=graph_version,
            graph_invocation_id=graph_invocation_id,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_TASK_CANCELLED,
            output_payload_uri_prefix=output_payload_uri_prefix,
        )

    @classmethod
    def function_executor_terminated(
        cls,
        task_id: str,
        namespace: str,
        graph_name: str,
        function_name: str,
        graph_version: str,
        graph_invocation_id: str,
        output_payload_uri_prefix: str,
    ) -> "TaskOutput":
        """Creates a TaskOutput for the case when task didn't run because its FE terminated."""
        return TaskOutput(
            task_id=task_id,
            namespace=namespace,
            graph_name=graph_name,
            function_name=function_name,
            graph_version=graph_version,
            graph_invocation_id=graph_invocation_id,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
            # TODO: add FE startup stdout, stderr to the task output if FE failed to startup.
            stdout="",
            stderr="Can't execute the function because its Function Executor terminated.",
            output_payload_uri_prefix=output_payload_uri_prefix,
        )
