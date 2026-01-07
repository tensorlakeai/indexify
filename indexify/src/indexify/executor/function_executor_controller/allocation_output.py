from typing import Any

from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationFailureReason as FEAllocationFailureReason,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationOutcomeCode as FEAllocationOutcomeCode,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationResult as FEAllocationResult,
)

from indexify.proto.executor_api_pb2 import (
    AllocationFailureReason,
    AllocationOutcomeCode,
    FunctionExecutorTerminationReason,
)


class AllocationOutput:
    """Result of running an allocation."""

    def __init__(
        self,
        outcome_code: AllocationOutcomeCode,
        failure_reason: AllocationFailureReason | None,
        fe_result: FEAllocationResult | None,
        function_outputs_blob_uri: str | None,
        execution_duration_ms: int | None,
    ):
        self.outcome_code: AllocationOutcomeCode = outcome_code
        self.failure_reason: AllocationFailureReason = failure_reason
        self.fe_result: FEAllocationResult | None = fe_result
        self.function_outputs_blob_uri: str | None = function_outputs_blob_uri
        self.execution_duration_ms: int | None = execution_duration_ms

    @classmethod
    def from_allocation_result(
        cls,
        fe_result: FEAllocationResult,
        function_outputs_blob_uri: str | None,
        execution_duration_ms: int,
        logger: Any,
    ) -> "AllocationOutput":
        """Creates a AllocationOutput from Function Executor's AllocationResult."""
        return AllocationOutput(
            outcome_code=_to_server_alloc_outcome_code(
                fe_alloc_outcome_code=fe_result.outcome_code, logger=logger
            ),
            failure_reason=(
                _to_server_alloc_failure_reason(
                    fe_alloc_failure_reason=fe_result.failure_reason, logger=logger
                )
                if fe_result.HasField("failure_reason")
                else None
            ),
            fe_result=fe_result,
            function_outputs_blob_uri=function_outputs_blob_uri,
            execution_duration_ms=execution_duration_ms,
        )

    @classmethod
    def internal_error(
        cls,
        execution_duration_ms: int | None,
    ) -> "AllocationOutput":
        """Creates a AllocationOutput for an internal error."""
        return AllocationOutput(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR,
            fe_result=None,
            function_outputs_blob_uri=None,
            execution_duration_ms=execution_duration_ms,
        )

    @classmethod
    def function_timeout(
        cls,
        execution_duration_ms: int,
    ) -> "AllocationOutput":
        """Creates a AllocationOutput for a function timeout error."""
        return AllocationOutput(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_TIMEOUT,
            fe_result=None,
            function_outputs_blob_uri=None,
            execution_duration_ms=execution_duration_ms,
        )

    @classmethod
    def function_error_with_healthy_function_executor(
        cls,
        execution_duration_ms: int,
    ) -> "AllocationOutput":
        """Creates a AllocationOutput for a function error with healthy Function Executor."""
        return AllocationOutput(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR,
            fe_result=None,
            function_outputs_blob_uri=None,
            execution_duration_ms=execution_duration_ms,
        )

    @classmethod
    def function_executor_is_in_undefined_state_after_running_allocation(
        cls,
        execution_duration_ms: int,
    ) -> "AllocationOutput":
        """Creates a AllocationOutput for an FE in undefined state after running allocation on it."""
        # When FE is unresponsive we don't know exact cause of the failure.
        return AllocationOutput(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            # Treat the grey failure as a function error and thus charge the customer.
            # This is to prevent service abuse by intentionally misbehaving functions.
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR,
            fe_result=None,
            function_outputs_blob_uri=None,
            execution_duration_ms=execution_duration_ms,
        )

    @classmethod
    def allocation_cancelled(
        cls,
        execution_duration_ms: int | None,
    ) -> "AllocationOutput":
        """Creates a AllocationOutput for the case when allocation was removed by Server.

        If allocation started already before the cancellation, its execution duration ms is set.
        """
        return AllocationOutput(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_ALLOCATION_CANCELLED,
            fe_result=None,
            function_outputs_blob_uri=None,
            execution_duration_ms=execution_duration_ms,
        )

    @classmethod
    def allocation_didn_run_because_function_executor_terminated(
        cls,
    ) -> "AllocationOutput":
        """Creates a AllocationOutput for the case when allocation didn't run because its FE terminated."""
        return AllocationOutput(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
            fe_result=None,
            function_outputs_blob_uri=None,
            execution_duration_ms=None,
        )

    @classmethod
    def allocation_ran_out_of_memory(
        cls,
        execution_duration_ms: int,
    ) -> "AllocationOutput":
        """Creates a AllocationOutput for the case when allocation failed because its FE ran out of memory."""
        return AllocationOutput(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=AllocationFailureReason.ALLOCATION_FAILURE_REASON_OOM,
            fe_result=None,
            function_outputs_blob_uri=None,
            execution_duration_ms=execution_duration_ms,
        )

    @classmethod
    def allocation_didn_run_because_function_executor_startup_failed(
        cls,
        fe_termination_reason: FunctionExecutorTerminationReason,
        logger: Any,
    ) -> "AllocationOutput":
        """Creates AllocationOutput for the case when we fail an allocation that didn't run because its FE startup failed."""
        return AllocationOutput(
            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
            failure_reason=_fe_startup_failure_reason_to_alloc_failure_reason(
                fe_termination_reason, logger
            ),
            fe_result=None,
            function_outputs_blob_uri=None,
            execution_duration_ms=None,
        )


def _fe_startup_failure_reason_to_alloc_failure_reason(
    fe_termination_reason: FunctionExecutorTerminationReason, logger: Any
) -> AllocationFailureReason:
    # Only need to check FE termination reasons happening on FE startup.
    if (
        fe_termination_reason
        == FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_ERROR
    ):
        return (
            AllocationFailureReason.ALLOCATION_FAILURE_REASON_STARTUP_FAILED_FUNCTION_ERROR
        )
    elif (
        fe_termination_reason
        == FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT
    ):
        return (
            AllocationFailureReason.ALLOCATION_FAILURE_REASON_STARTUP_FAILED_FUNCTION_TIMEOUT
        )
    elif (
        fe_termination_reason
        == FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_STARTUP_FAILED_INTERNAL_ERROR
    ):
        return (
            AllocationFailureReason.ALLOCATION_FAILURE_REASON_STARTUP_FAILED_INTERNAL_ERROR
        )
    elif (
        fe_termination_reason
        == FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_CANCELLED
    ):
        return (
            AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED
        )
    elif (
        fe_termination_reason
        == FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_OOM
    ):
        return AllocationFailureReason.ALLOCATION_FAILURE_REASON_OOM
    else:
        logger.error(
            "unexpected function executor startup failure reason",
            fe_termination_reason=FunctionExecutorTerminationReason.Name(
                fe_termination_reason
            ),
        )
        return AllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR


def _to_server_alloc_outcome_code(
    fe_alloc_outcome_code: FEAllocationOutcomeCode, logger
) -> AllocationOutcomeCode:
    if fe_alloc_outcome_code == FEAllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS:
        return AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS
    elif (
        fe_alloc_outcome_code == FEAllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE
    ):
        return AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE
    else:
        logger.warning(
            "unknown AllocationOutcomeCode received from Function Executor",
            value=FEAllocationOutcomeCode.Name(fe_alloc_outcome_code),
        )
        return AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_UNKNOWN


def _to_server_alloc_failure_reason(
    fe_alloc_failure_reason: FEAllocationFailureReason, logger: Any
) -> AllocationFailureReason:
    if (
        fe_alloc_failure_reason
        == FEAllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR
    ):
        return AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR
    elif (
        fe_alloc_failure_reason
        == FEAllocationFailureReason.ALLOCATION_FAILURE_REASON_REQUEST_ERROR
    ):
        return AllocationFailureReason.ALLOCATION_FAILURE_REASON_REQUEST_ERROR
    elif (
        fe_alloc_failure_reason
        == FEAllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR
    ):
        # Internal FE error is treated as function error because we don't know what
        # exactly happened on FE side, could be service abuse attempt by user code.
        return AllocationFailureReason.ALLOCATION_FAILURE_REASON_FUNCTION_ERROR
    else:
        logger.warning(
            "unknown AllocationFailureReason received from Function Executor",
            value=FEAllocationFailureReason.Name(fe_alloc_failure_reason),
        )
        return AllocationFailureReason.ALLOCATION_FAILURE_REASON_UNKNOWN
