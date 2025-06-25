from typing import Dict, List, Optional

from tensorlake.function_executor.proto.function_executor_pb2 import (
    SerializedObject,
)

from indexify.proto.executor_api_pb2 import (
    DataPayload,
    TaskAllocation,
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
        allocation: TaskAllocation,
        outcome_code: TaskOutcomeCode,
        # Optional[TaskFailureReason] is not supported in python 3.9
        failure_reason: TaskFailureReason = None,
        invocation_error_output: Optional[SerializedObject] = None,
        function_outputs: List[SerializedObject] = [],
        next_functions: List[str] = [],
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        reducer: bool = False,
        metrics: Optional[TaskMetrics] = None,
    ):
        self.task = allocation.task
        self.allocation = allocation
        self.function_outputs = function_outputs
        self.next_functions = next_functions
        self.stdout = stdout
        self.stderr = stderr
        self.reducer = reducer
        self.outcome_code = outcome_code
        self.failure_reason = failure_reason
        self.invocation_error_output = invocation_error_output
        self.metrics = metrics
        self.uploaded_data_payloads: List[DataPayload] = []
        self.uploaded_stdout: Optional[DataPayload] = None
        self.uploaded_stderr: Optional[DataPayload] = None
        self.uploaded_invocation_error_output: Optional[DataPayload] = None

    @classmethod
    def internal_error(
        cls,
        allocation: TaskAllocation,
    ) -> "TaskOutput":
        """Creates a TaskOutput for an internal error."""
        # We are not sharing internal error messages with the customer.
        return TaskOutput(
            allocation=allocation,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_INTERNAL_ERROR,
            stderr="Platform failed to execute the function.",
        )

    @classmethod
    def function_timeout(
        cls,
        allocation: TaskAllocation,
        timeout_sec: float,
    ) -> "TaskOutput":
        """Creates a TaskOutput for an function timeout error."""
        # Task stdout, stderr is not available.
        return TaskOutput(
            allocation=allocation,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_TIMEOUT,
            stderr=f"Function exceeded its configured timeout of {timeout_sec:.3f} sec.",
        )

    @classmethod
    def task_cancelled(
        cls,
        allocation: TaskAllocation,
    ) -> "TaskOutput":
        """Creates a TaskOutput for the case when task didn't finish because its allocation was removed by Server."""
        return TaskOutput(
            allocation=allocation,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_TASK_CANCELLED,
        )

    @classmethod
    def function_executor_terminated(
        cls,
        allocation: TaskAllocation,
    ) -> "TaskOutput":
        """Creates a TaskOutput for the case when task didn't run because its FE terminated."""
        return TaskOutput(
            allocation=allocation,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
        )
