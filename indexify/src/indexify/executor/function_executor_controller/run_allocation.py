import asyncio
import time
from typing import Any

import grpc
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    Allocation as FEAllocation,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationFailureReason as FEAllocationFailureReason,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationOutcomeCode as FEAllocationOutcomeCode,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationResult,
    AwaitAllocationProgress,
    AwaitAllocationRequest,
    CreateAllocationRequest,
    DeleteAllocationRequest,
    SerializedObjectInsideBLOB,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.message_validator import MessageValidator

from indexify.executor.function_executor.function_executor import FunctionExecutor
from indexify.executor.function_executor.health_checker import HealthCheckResult
from indexify.proto.executor_api_pb2 import (
    Allocation,
    AllocationFailureReason,
    AllocationOutcomeCode,
    FunctionExecutorTerminationReason,
)

from .allocation_info import AllocationInfo
from .allocation_output import AllocationMetrics, AllocationOutput
from .events import AllocationExecutionFinished
from .metrics.run_allocation import (
    metric_function_executor_run_allocation_rpc_errors,
    metric_function_executor_run_allocation_rpc_latency,
    metric_function_executor_run_allocation_rpcs,
    metric_function_executor_run_allocation_rpcs_in_progress,
)

_CREATE_ALLOCATION_TIMEOUT_SECS = 5
_DELETE_ALLOCATION_TIMEOUT_SECS = 5


async def run_allocation_on_function_executor(
    alloc_info: AllocationInfo, function_executor: FunctionExecutor, logger: Any
) -> AllocationExecutionFinished:
    """Runs the allocation on the Function Executor and sets alloc_info.output with the result.

    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)

    if alloc_info.input is None:
        logger.error(
            "allocation input is None, this should never happen",
        )
        alloc_info.output = AllocationOutput.internal_error(
            allocation=alloc_info.allocation,
            execution_start_time=None,
            execution_end_time=None,
        )
        return AllocationExecutionFinished(
            alloc_info=alloc_info,
            function_executor_termination_reason=None,
        )

    fe_alloc: FEAllocation = FEAllocation(
        request_id=alloc_info.allocation.request_id,
        function_call_id=alloc_info.allocation.function_call_id,
        allocation_id=alloc_info.allocation.allocation_id,
        inputs=alloc_info.input.function_inputs,
    )

    function_executor.request_state_client().add_allocation_to_request_id_entry(
        allocation_id=alloc_info.allocation.allocation_id,
        request_id=alloc_info.allocation.request_id,
    )

    metric_function_executor_run_allocation_rpcs.inc()
    metric_function_executor_run_allocation_rpcs_in_progress.inc()
    # Not None if the Function Executor should be terminated after running the alloc.
    function_executor_termination_reason: FunctionExecutorTerminationReason | None = (
        None
    )

    # NB: We start this timer before invoking the first RPC, since
    # user code should be executing by the time the create_allocation() RPC
    # returns, so not attributing the allocation management RPC overhead to
    # the user would open a possibility for abuse. (This is somewhat
    # mitigated by the fact that these RPCs should have a very low
    # overhead.)
    execution_start_time: float = time.monotonic()

    # If this RPC failed due to customer code crashing the server we won't be
    # able to detect this. We'll treat this as our own error for now and thus
    # let the AioRpcError to be raised here.
    timeout_sec: float = alloc_info.allocation_timeout_ms / 1000.0
    try:
        # This aio task can only be cancelled during this await call.
        alloc_result: AllocationResult = await _run_allocation_rpcs(
            fe_alloc, function_executor, timeout_sec
        )
        alloc_info.output = _allocation_output_from_fe_result(
            allocation=alloc_info.allocation,
            result=alloc_result,
            execution_start_time=execution_start_time,
            execution_end_time=time.monotonic(),
            logger=logger,
        )
    except asyncio.TimeoutError:
        # This is an await_allocation() RPC timeout - we're not getting
        # progress messages or an allocation completion.
        function_executor_termination_reason = (
            FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_TIMEOUT
        )
        alloc_info.output = AllocationOutput.function_timeout(
            allocation=alloc_info.allocation,
            execution_start_time=execution_start_time,
            execution_end_time=time.monotonic(),
        )
    except grpc.aio.AioRpcError as e:
        # This indicates some sort of problem communicating with the FE.
        #
        # NB: We charge the user in these situations: code within the
        # FE is not isolated, so not charging would enable abuse.
        #
        # This is an unexpected situation, though, so we make sure to
        # log the situation for further investigation.

        function_executor_termination_reason = (
            FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_UNHEALTHY
        )
        metric_function_executor_run_allocation_rpc_errors.inc()

        if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            # This is either a create_allocation() RPC timeout or a
            # delete_allocation() RPC timeout; either suggests that the FE
            # is unhealthy.
            logger.error(
                "allocation management RPC execution deadline exceeded", exc_info=e
            )
        else:
            # This is a status from an unsuccessful RPC; this
            # shouldn't happen, but we handle it.
            logger.error("allocation management RPC failed", exc_info=e)

        alloc_info.output = AllocationOutput.function_executor_unresponsive(
            allocation=alloc_info.allocation,
            execution_start_time=execution_start_time,
            execution_end_time=time.monotonic(),
        )
    except asyncio.CancelledError:
        # Handle aio task cancellation during `await _run_allocation_rpcs`.
        # The allocation is still running in FE, we only cancelled the client-side RPC.
        function_executor_termination_reason = (
            FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_CANCELLED
        )
        alloc_info.output = AllocationOutput.allocation_cancelled(
            allocation=alloc_info.allocation,
            execution_start_time=execution_start_time,
            execution_end_time=time.monotonic(),
        )
    # TODO: Handle ValueError here separately. It happens when FE returned an invalid response
    # that doesn't pass validation. Still charge the user in this case to prevent service abuse.
    except Exception as e:
        # This is an unexpected exception; we believe that this
        # indicates an internal error.
        logger.error(
            "unexpected internal error during allocation lifecycle RPC sequence",
            exc_info=e,
        )
        alloc_info.output = AllocationOutput.internal_error(
            allocation=alloc_info.allocation,
            execution_start_time=execution_start_time,
            execution_end_time=time.monotonic(),
        )

    metric_function_executor_run_allocation_rpc_latency.observe(
        time.monotonic() - execution_start_time
    )
    metric_function_executor_run_allocation_rpcs_in_progress.dec()

    function_executor.request_state_client().remove_allocation_to_request_id_entry(
        allocation_id=alloc_info.allocation.allocation_id,
    )

    if (
        alloc_info.output.outcome_code
        == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE
        and function_executor_termination_reason is None
    ):
        try:
            # Check if the allocation failed because the FE is unhealthy to prevent more allocations failing.
            result: HealthCheckResult = await function_executor.health_checker().check()
            if not result.is_healthy:
                function_executor_termination_reason = (
                    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_UNHEALTHY
                )
                logger.error(
                    "Function Executor health check failed after running allocation, shutting down Function Executor",
                    health_check_fail_reason=result.reason,
                )
        except asyncio.CancelledError:
            # The aio task was cancelled during the health check await.
            # We can't conclude anything about the health of the FE here.
            pass

    _log_alloc_execution_finished(output=alloc_info.output, logger=logger)

    return AllocationExecutionFinished(
        alloc_info=alloc_info,
        function_executor_termination_reason=function_executor_termination_reason,
    )


async def _run_allocation_rpcs(
    alloc: Allocation, function_executor: FunctionExecutor, timeout_sec: float
) -> AllocationResult:
    """Runs the allocation, returning the result, reporting errors via exceptions."""
    alloc_result: AllocationResult | None = None
    channel: grpc.aio.Channel = function_executor.channel()
    fe_stub = FunctionExecutorStub(channel)

    # Create allocation with timeout
    await fe_stub.create_allocation(
        CreateAllocationRequest(allocation=alloc),
        timeout=_CREATE_ALLOCATION_TIMEOUT_SECS,
    )

    # Await allocation with timeout resets on each response
    await_rpc = fe_stub.await_allocation(
        AwaitAllocationRequest(allocation_id=alloc.allocation_id)
    )

    try:
        while True:
            # Wait for next response with fresh timeout each time
            response: AwaitAllocationProgress = await asyncio.wait_for(
                await_rpc.read(), timeout=timeout_sec
            )

            if response == grpc.aio.EOF:
                break
            elif response.WhichOneof("response") == "allocation_result":
                alloc_result = response.allocation_result
                break

            # NB: We don't actually check for other message types
            # here; any message from the FE is treated as an
            # indication that it's making forward progress.
    finally:
        # Cancel the outstanding RPC to ensure any resources in use
        # are cleaned up; note that this is idempotent (in case the
        # RPC has already completed).
        await_rpc.cancel()

    # Delete allocation with timeout
    await fe_stub.delete_allocation(
        DeleteAllocationRequest(allocation_id=alloc.allocation_id),
        timeout=_DELETE_ALLOCATION_TIMEOUT_SECS,
    )

    if alloc_result is None:
        raise grpc.aio.AioRpcError(
            grpc.StatusCode.CANCELLED,
            None,
            None,
            "Function Executor didn't return allocation result",
        )

    return alloc_result


def _allocation_output_from_fe_result(
    allocation: Allocation,
    result: AllocationResult,
    execution_start_time: float,
    execution_end_time: float | None,
    logger: Any,
) -> AllocationOutput:
    response_validator = MessageValidator(result)
    response_validator.required_field("outcome_code")

    metrics = AllocationMetrics(counters={}, timers={})
    if result.HasField("metrics"):
        # Can be None if e.g. function failed.
        metrics.counters = dict(result.metrics.counters)
        metrics.timers = dict(result.metrics.timers)

    outcome_code: AllocationOutcomeCode = _to_alloc_outcome_code(
        result.outcome_code, logger=logger
    )
    failure_reason: AllocationFailureReason | None = None
    request_error_output: SerializedObjectInsideBLOB | None = None
    uploaded_request_error_blob: BLOB | None = None

    if outcome_code == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE:
        response_validator.required_field("failure_reason")
        failure_reason: AllocationFailureReason | None = _to_alloc_failure_reason(
            result.failure_reason, logger
        )
        if (
            failure_reason
            == AllocationFailureReason.ALLOCATION_FAILURE_REASON_REQUEST_ERROR
        ):
            response_validator.required_field("request_error_output")
            response_validator.required_field("uploaded_request_error_blob")
            request_error_output = result.request_error_output
            uploaded_request_error_blob = result.uploaded_request_error_blob
    elif outcome_code == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS:
        if not (result.HasField("value") or result.HasField("updates")):
            raise ValueError(
                "Function Executor returned AllocationResult with `success` outcome code but neither `value` nor `updates` are set in it"
            )
        response_validator.required_field("uploaded_function_outputs_blob")

    return AllocationOutput(
        allocation=allocation,
        outcome_code=outcome_code,
        failure_reason=failure_reason,
        output_value=result.value if result.HasField("value") else None,
        output_execution_plan_updates=(
            result.updates if result.HasField("updates") else None
        ),
        uploaded_function_outputs_blob=result.uploaded_function_outputs_blob,
        request_error_output=request_error_output,
        uploaded_request_error_blob=uploaded_request_error_blob,
        metrics=metrics,
        execution_start_time=execution_start_time,
        execution_end_time=execution_end_time,
    )


def _log_alloc_execution_finished(output: AllocationOutput, logger: Any) -> None:
    logger.info(
        "finished running allocation",
        success=output.outcome_code
        == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
        outcome_code=AllocationOutcomeCode.Name(output.outcome_code),
        failure_reason=(
            AllocationFailureReason.Name(output.failure_reason)
            if output.failure_reason is not None
            else None
        ),
    )


def _to_alloc_outcome_code(
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


def _to_alloc_failure_reason(
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
        return AllocationFailureReason.ALLOCATION_FAILURE_REASON_INTERNAL_ERROR
    else:
        logger.warning(
            "unknown AllocationFailureReason received from Function Executor",
            value=FEAllocationFailureReason.Name(fe_alloc_failure_reason),
        )
        return AllocationFailureReason.ALLOCATION_FAILURE_REASON_UNKNOWN
