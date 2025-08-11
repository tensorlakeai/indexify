from typing import Any, Dict, List, Optional

from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    SerializedObjectInsideBLOB,
)

from indexify.proto.executor_api_pb2 import (
    FunctionExecutorTerminationReason,
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
        failure_reason: TaskFailureReason | None = None,
        function_outputs: List[SerializedObjectInsideBLOB] = [],
        uploaded_function_outputs_blob: Optional[BLOB] = None,
        invocation_error_output: Optional[SerializedObjectInsideBLOB] = None,
        uploaded_invocation_error_blob: Optional[BLOB] = None,
        next_functions: List[str] = [],
        metrics: Optional[TaskMetrics] = None,
        execution_start_time: Optional[float] = None,
        execution_end_time: Optional[float] = None,
    ):
        self.allocation = allocation
        self.outcome_code = outcome_code
        self.failure_reason = failure_reason
        self.function_outputs = function_outputs
        self.uploaded_function_outputs_blob = uploaded_function_outputs_blob
        self.invocation_error_output = invocation_error_output
        self.uploaded_invocation_error_blob = uploaded_invocation_error_blob
        self.next_functions = next_functions
        self.metrics = metrics
        self.execution_start_time = execution_start_time
        self.execution_end_time = execution_end_time

    @classmethod
    def internal_error(
        cls,
        allocation: TaskAllocation,
        execution_start_time: Optional[float],
        execution_end_time: Optional[float],
    ) -> "TaskOutput":
        """Creates a TaskOutput for an internal error."""
        return TaskOutput(
            allocation=allocation,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_INTERNAL_ERROR,
            execution_start_time=execution_start_time,
            execution_end_time=execution_end_time,
        )

    @classmethod
    def function_timeout(
        cls,
        allocation: TaskAllocation,
        execution_start_time: Optional[float],
        execution_end_time: Optional[float],
    ) -> "TaskOutput":
        """Creates a TaskOutput for a function timeout error."""
        return TaskOutput(
            allocation=allocation,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_TIMEOUT,
            execution_start_time=execution_start_time,
            execution_end_time=execution_end_time,
        )

    @classmethod
    def function_executor_unresponsive(
        cls,
        allocation: TaskAllocation,
        execution_start_time: Optional[float],
        execution_end_time: Optional[float],
    ) -> "TaskOutput":
        """Creates a TaskOutput for an unresponsive FE aka grey failure."""
        # When FE is unresponsive we don't know exact cause of the failure.
        return TaskOutput(
            allocation=allocation,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            # Treat the grey failure as a function error and thus charge the customer.
            # This is to prevent service abuse by intentionally misbehaving functions.
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_ERROR,
            execution_start_time=execution_start_time,
            execution_end_time=execution_end_time,
        )

    @classmethod
    def task_cancelled(
        cls,
        allocation: TaskAllocation,
        execution_start_time: Optional[float],
        execution_end_time: Optional[float],
    ) -> "TaskOutput":
        """Creates a TaskOutput for the case when task didn't finish because its allocation was removed by Server."""
        return TaskOutput(
            allocation=allocation,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=TaskFailureReason.TASK_FAILURE_REASON_TASK_CANCELLED,
            execution_start_time=execution_start_time,
            execution_end_time=execution_end_time,
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
        fe_termination_reason: FunctionExecutorTerminationReason,
        logger: Any,
    ) -> "TaskOutput":
        """Creates a TaskOutput for the case when we fail a task that didn't run because its FE startup failed."""
        return TaskOutput(
            allocation=allocation,
            outcome_code=TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE,
            failure_reason=_fe_startup_failure_reason_to_task_failure_reason(
                fe_termination_reason, logger
            ),
        )


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
    elif (
        fe_termination_reason
        == FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_CANCELLED
    ):
        # This fe termination reason is used when FE gets deleted by Server from desired state while it's starting up.
        return TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED
    else:
        logger.error(
            "unexpected function executor startup failure reason",
            fe_termination_reason=FunctionExecutorTerminationReason.Name(
                fe_termination_reason
            ),
        )
        return TaskFailureReason.TASK_FAILURE_REASON_INTERNAL_ERROR
