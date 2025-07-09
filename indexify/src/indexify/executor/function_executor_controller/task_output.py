from typing import Any, Dict, List, Optional

from tensorlake.function_executor.proto.function_executor_pb2 import (
    SerializedObject,
)

from indexify.proto.executor_api_pb2 import (
    DataPayload,
    FunctionExecutorTerminationReason,
    TaskAllocation,
    TaskFailureReason,
    TaskOutcomeCode,
)

from .function_executor_startup_output import FunctionExecutorStartupOutput


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
        metrics: Optional[TaskMetrics] = None,
    ):
        self.task = allocation.task
        self.allocation = allocation
        self.function_outputs = function_outputs
        self.next_functions = next_functions
        self.stdout = stdout
        self.stderr = stderr
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

    @classmethod
    def function_executor_startup_failed(
        cls,
        allocation: TaskAllocation,
        fe_startup_output: FunctionExecutorStartupOutput,
        logger: Any,
    ) -> "TaskOutput":
        """Creates a TaskOutput for the case when we fail a task because its FE startup failed."""
        output = TaskOutput(
            allocation=allocation,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=_fe_startup_failure_reason_to_task_failure_reason(
                fe_startup_output.termination_reason, logger
            ),
        )
        # Use FE startup stdout, stderr for allocations that we failed because FE startup failed.
        output.uploaded_stdout = fe_startup_output.stdout
        output.uploaded_stderr = fe_startup_output.stderr
        return output


def _fe_startup_failure_reason_to_task_failure_reason(
    fe_termination_reason: FunctionExecutorTerminationReason, logger: Any
) -> TaskFailureReason:
    # Only need to check FE termination reasons happening on FE startup.
    if (
        fe_termination_reason
        == FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR
    ):
        return TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_ERROR
    elif (
        fe_termination_reason
        == FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT
    ):
        return TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_TIMEOUT
    elif (
        fe_termination_reason
        == FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_INTERNAL_ERROR
    ):
        return TaskFailureReason.TASK_FAILURE_REASON_INTERNAL_ERROR
    else:
        logger.error(
            "unexpected function executor startup failure reason",
            fe_termination_reason=FunctionExecutorTerminationReason.Name(
                fe_termination_reason
            ),
        )
        return TaskFailureReason.TASK_FAILURE_REASON_UNKNOWN
