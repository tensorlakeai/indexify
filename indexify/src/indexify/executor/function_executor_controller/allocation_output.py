from typing import Any, Dict

from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    ExecutionPlanUpdates,
    SerializedObjectInsideBLOB,
)

from indexify.proto.executor_api_pb2 import (
    Allocation,
    AllocationFailureReason,
    AllocationOutcomeCode,
    FunctionExecutorTerminationReason,
)


class AllocationMetrics:
    """Metrics for an allocation."""

    def __init__(self, counters: Dict[str, int], timers: Dict[str, float]):
        self.counters = counters
        self.timers = timers


class AllocationOutput:
    """Result of running an allocation."""

    def __init__(
        self,
        allocation: Allocation,
        outcome_code: AllocationOutcomeCode,
        failure_reason: AllocationFailureReason | None = None,
        output_value: SerializedObjectInsideBLOB | None = None,
        output_execution_plan_updates: ExecutionPlanUpdates | None = None,
        uploaded_function_outputs_blob: BLOB | None = None,
        request_error_output: SerializedObjectInsideBLOB | None = None,
        uploaded_request_error_blob: BLOB | None = None,
        metrics: AllocationMetrics | None = None,
        execution_start_time: float | None = None,
        execution_end_time: float | None = None,
    ):
        self.allocation = allocation
        self.outcome_code = outcome_code
        self.failure_reason = failure_reason
        self.output_value = output_value
        self.output_execution_plan_updates = output_execution_plan_updates
        self.uploaded_function_outputs_blob = uploaded_function_outputs_blob
        self.request_error_output = request_error_output
        self.uploaded_request_error_blob = uploaded_request_error_blob
        self.metrics = metrics
        self.execution_start_time = execution_start_time
        self.execution_end_time = execution_end_time

    @classmethod
    def internal_error(
        cls,
        allocation: Allocation,
        execution_start_time: float | None,
        execution_end_time: float | None,
    ) -> "AllocationOutput":
        """Creates a AllocationOutput for an internal error."""
        return AllocationOutput(
            allocation=allocation,
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR,
            execution_start_time=execution_start_time,
            execution_end_time=execution_end_time,
        )

    @classmethod
    def function_timeout(
        cls,
        allocation: Allocation,
        execution_start_time: float | None,
        execution_end_time: float | None,
    ) -> "AllocationOutput":
        """Creates a AllocationOutput for a function timeout error."""
        return AllocationOutput(
            allocation=allocation,
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_TIMEOUT,
            execution_start_time=execution_start_time,
            execution_end_time=execution_end_time,
        )

    @classmethod
    def function_executor_unresponsive(
        cls,
        allocation: Allocation,
        execution_start_time: float | None,
        execution_end_time: float | None,
    ) -> "AllocationOutput":
        """Creates a AllocationOutput for an unresponsive FE aka grey failure."""
        # When FE is unresponsive we don't know exact cause of the failure.
        return AllocationOutput(
            allocation=allocation,
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            # Treat the grey failure as a function error and thus charge the customer.
            # This is to prevent service abuse by intentionally misbehaving functions.
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR,
            execution_start_time=execution_start_time,
            execution_end_time=execution_end_time,
        )

    @classmethod
    def allocation_cancelled(
        cls,
        allocation: Allocation,
        execution_start_time: float | None,
        execution_end_time: float | None,
    ) -> "AllocationOutput":
        """Creates a AllocationOutput for the case when allocation didn't finish because its allocation was removed by Server."""
        return AllocationOutput(
            allocation=allocation,
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_ALLOCATION_CANCELLED,
            execution_start_time=execution_start_time,
            execution_end_time=execution_end_time,
        )

    @classmethod
    def function_executor_terminated(
        cls,
        allocation: Allocation,
    ) -> "AllocationOutput":
        """Creates a AllocationOutput for the case when allocation didn't run because its FE terminated."""
        return AllocationOutput(
            allocation=allocation,
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
        )

    @classmethod
    def function_executor_startup_failed(
        cls,
        allocation: Allocation,
        fe_termination_reason: FunctionExecutorTerminationReason,
        logger: Any,
    ) -> "AllocationOutput":
        """Creates AllocationOutput for the case when we fail an allocation that didn't run because its FE startup failed."""
        return AllocationOutput(
            allocation=allocation,
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=_fe_startup_failure_reason_to_alloc_failure_reason(
                fe_termination_reason, logger
            ),
        )


def _fe_startup_failure_reason_to_alloc_failure_reason(
    fe_termination_reason: FunctionExecutorTerminationReason, logger: Any
) -> AllocationFailureReason:
    # Only need to check FE termination reasons happening on FE startup.
    if (
        fe_termination_reason
        == FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR
    ):
        return AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR
    elif (
        fe_termination_reason
        == FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT
    ):
        return AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_TIMEOUT
    elif (
        fe_termination_reason
        == FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_INTERNAL_ERROR
    ):
        return AllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR
    elif (
        fe_termination_reason
        == FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_CANCELLED
    ):
        # This fe termination reason is used when FE gets deleted by Server from desired state while it's starting up.
        return (
            AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED
        )
    else:
        logger.error(
            "unexpected function executor startup failure reason",
            fe_termination_reason=FunctionExecutorTerminationReason.Name(
                fe_termination_reason
            ),
        )
        return AllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR
