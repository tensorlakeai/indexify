from typing import Dict, List, Optional

from tensorlake.function_executor.proto.function_executor_pb2 import (
    FunctionOutput,
    RouterOutput,
)

from indexify.proto.executor_api_pb2 import (
    DataPayload,
    Task,
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
        task: Task,
        allocation_id: str,
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
        self.task = task
        self.allocation_id = allocation_id
        self.function_output = function_output
        self.router_output = router_output
        self.stdout = stdout
        self.stderr = stderr
        self.reducer = reducer
        self.outcome_code = outcome_code
        self.failure_reason = failure_reason
        self.metrics = metrics
        self.output_encoding = output_encoding
        self.uploaded_data_payloads = uploaded_data_payloads
        self.uploaded_stdout = uploaded_stdout
        self.uploaded_stderr = uploaded_stderr

    @classmethod
    def internal_error(
        cls,
        task: Task,
        allocation_id: str,
    ) -> "TaskOutput":
        """Creates a TaskOutput for an internal error."""
        # We are not sharing internal error messages with the customer.
        return TaskOutput(
            task=task,
            allocation_id=allocation_id,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_INTERNAL_ERROR,
            stderr="Platform failed to execute the function.",
        )

    @classmethod
    def function_timeout(
        cls,
        task: Task,
        allocation_id: str,
        timeout_sec: float,
    ) -> "TaskOutput":
        """Creates a TaskOutput for an function timeout error."""
        # Task stdout, stderr is not available.
        return TaskOutput(
            task=task,
            allocation_id=allocation_id,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_TIMEOUT,
            stderr=f"Function exceeded its configured timeout of {timeout_sec:.3f} sec.",
        )

    @classmethod
    def task_cancelled(
        cls,
        task: Task,
        allocation_id: str,
    ) -> "TaskOutput":
        """Creates a TaskOutput for the case when task didn't finish because its allocation was removed by Server."""
        return TaskOutput(
            task=task,
            allocation_id=allocation_id,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_TASK_CANCELLED,
        )

    @classmethod
    def function_executor_terminated(
        cls,
        task: Task,
        allocation_id: str,
    ) -> "TaskOutput":
        """Creates a TaskOutput for the case when task didn't run because its FE terminated."""
        return TaskOutput(
            task=task,
            allocation_id=allocation_id,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
            # TODO: add FE startup stdout, stderr to the task output if FE failed to startup.
            stdout="",
            stderr="Can't execute the function because its Function Executor terminated.",
        )
