import asyncio
import time
from typing import Any, Optional

import grpc
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    AwaitTaskProgress,
    AwaitTaskRequest,
    CreateTaskRequest,
    DeleteTaskRequest,
    SerializedObjectInsideBLOB,
    Task,
    TaskDiagnostics,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    TaskFailureReason as FETaskFailureReason,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    TaskOutcomeCode as FETaskOutcomeCode,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    TaskResult,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.message_validator import MessageValidator

from indexify.executor.function_executor.function_executor import FunctionExecutor
from indexify.executor.function_executor.health_checker import HealthCheckResult
from indexify.proto.executor_api_pb2 import (
    FunctionExecutorTerminationReason,
    TaskAllocation,
    TaskFailureReason,
    TaskOutcomeCode,
)

from .events import TaskExecutionFinished
from .metrics.run_task import (
    metric_function_executor_run_task_rpc_errors,
    metric_function_executor_run_task_rpc_latency,
    metric_function_executor_run_task_rpcs,
    metric_function_executor_run_task_rpcs_in_progress,
)
from .task_info import TaskInfo
from .task_output import TaskMetrics, TaskOutput

_CREATE_TASK_TIMEOUT_SECS = 5
_DELETE_TASK_TIMEOUT_SECS = 5


async def run_task_on_function_executor(
    task_info: TaskInfo, function_executor: FunctionExecutor, logger: Any
) -> TaskExecutionFinished:
    """Runs the task on the Function Executor and sets task_info.output with the result.

    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)

    if task_info.input is None:
        logger.error(
            "task input is None, this should never happen",
        )
        task_info.output = TaskOutput.internal_error(
            allocation=task_info.allocation,
            execution_start_time=None,
            execution_end_time=None,
        )
        return TaskExecutionFinished(
            task_info=task_info,
            function_executor_termination_reason=None,
        )

    task = Task(
        namespace=task_info.allocation.task.namespace,
        graph_name=task_info.allocation.task.graph_name,
        graph_version=task_info.allocation.task.graph_version,
        function_name=task_info.allocation.task.function_name,
        graph_invocation_id=task_info.allocation.task.graph_invocation_id,
        task_id=task_info.allocation.task.id,
        allocation_id=task_info.allocation.allocation_id,
        request=task_info.input.function_inputs,
    )

    function_executor.invocation_state_client().add_task_to_invocation_id_entry(
        task_id=task_info.allocation.task.id,
        invocation_id=task_info.allocation.task.graph_invocation_id,
    )

    metric_function_executor_run_task_rpcs.inc()
    metric_function_executor_run_task_rpcs_in_progress.inc()
    # Not None if the Function Executor should be terminated after running the task.
    function_executor_termination_reason: Optional[
        FunctionExecutorTerminationReason
    ] = None

    # NB: We start this timer before invoking the first RPC, since
    # user code should be executing by the time the create_task() RPC
    # returns, so not attributing the task management RPC overhead to
    # the user would open a possibility for abuse. (This is somewhat
    # mitigated by the fact that these RPCs should have a very low
    # overhead.)
    execution_start_time: Optional[float] = time.monotonic()

    # If this RPC failed due to customer code crashing the server we won't be
    # able to detect this. We'll treat this as our own error for now and thus
    # let the AioRpcError to be raised here.
    timeout_sec: float = task_info.allocation.task.timeout_ms / 1000.0
    try:
        # This aio task can only be cancelled during this await call.
        task_result = await _run_task_rpcs(task, function_executor, timeout_sec)

        _process_task_diagnostics(task_result.diagnostics, logger)

        task_info.output = _task_output_from_function_executor_result(
            allocation=task_info.allocation,
            result=task_result,
            execution_start_time=execution_start_time,
            execution_end_time=time.monotonic(),
            logger=logger,
        )
    except asyncio.TimeoutError:
        # This is an await_task() RPC timeout - we're not getting
        # progress messages or a task completion.
        function_executor_termination_reason = (
            FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_TIMEOUT
        )
        task_info.output = TaskOutput.function_timeout(
            allocation=task_info.allocation,
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
        metric_function_executor_run_task_rpc_errors.inc()

        if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            # This is either a create_task() RPC timeout or a
            # delete_task() RPC timeout; either suggests that the FE
            # is unhealthy.
            logger.error("task management RPC execution deadline exceeded", exc_info=e)
        else:
            # This is a status from an unsuccessful RPC; this
            # shouldn't happen, but we handle it.
            logger.error("task management RPC failed", exc_info=e)

        task_info.output = TaskOutput.function_executor_unresponsive(
            allocation=task_info.allocation,
            execution_start_time=execution_start_time,
            execution_end_time=time.monotonic(),
        )
    except asyncio.CancelledError:
        # Handle aio task cancellation during `await _run_task_rpcs`.
        # The task is still running in FE, we only cancelled the client-side RPC.
        function_executor_termination_reason = (
            FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_CANCELLED
        )
        task_info.output = TaskOutput.task_cancelled(
            allocation=task_info.allocation,
            execution_start_time=execution_start_time,
            execution_end_time=time.monotonic(),
        )
    except Exception as e:
        # This is an unexpected exception; we believe that this
        # indicates an internal error.
        logger.error(
            "unexpected internal error during task lifecycle RPC sequence", exc_info=e
        )
        task_info.output = TaskOutput.internal_error(
            allocation=task_info.allocation,
            execution_start_time=execution_start_time,
            execution_end_time=time.monotonic(),
        )

    metric_function_executor_run_task_rpc_latency.observe(
        time.monotonic() - execution_start_time
    )
    metric_function_executor_run_task_rpcs_in_progress.dec()

    function_executor.invocation_state_client().remove_task_to_invocation_id_entry(
        task_id=task_info.allocation.task.id,
    )

    if (
        task_info.output.outcome_code == TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE
        and function_executor_termination_reason is None
    ):
        try:
            # Check if the task failed because the FE is unhealthy to prevent more tasks failing.
            result: HealthCheckResult = await function_executor.health_checker().check()
            if not result.is_healthy:
                function_executor_termination_reason = (
                    FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_UNHEALTHY
                )
                logger.error(
                    "Function Executor health check failed after running task, shutting down Function Executor",
                    health_check_fail_reason=result.reason,
                )
        except asyncio.CancelledError:
            # The aio task was cancelled during the health check await.
            # We can't conclude anything about the health of the FE here.
            pass

    _log_task_execution_finished(output=task_info.output, logger=logger)

    return TaskExecutionFinished(
        task_info=task_info,
        function_executor_termination_reason=function_executor_termination_reason,
    )


async def _run_task_rpcs(
    task: Task, function_executor: FunctionExecutor, timeout_sec: float
) -> TaskResult:
    """Runs the task, returning the result, reporting errors via exceptions."""
    task_result: Optional[TaskResult] = None
    channel: grpc.aio.Channel = function_executor.channel()
    fe_stub = FunctionExecutorStub(channel)

    # Create task with timeout
    await fe_stub.create_task(
        CreateTaskRequest(task=task), timeout=_CREATE_TASK_TIMEOUT_SECS
    )

    # Await task with timeout resets on each response
    await_rpc = fe_stub.await_task(AwaitTaskRequest(task_id=task.task_id))

    try:
        while True:
            # Wait for next response with fresh timeout each time
            response: AwaitTaskProgress = await asyncio.wait_for(
                await_rpc.read(), timeout=timeout_sec
            )

            if response == grpc.aio.EOF:
                break
            elif response.WhichOneof("response") == "task_result":
                task_result = response.task_result
                break

            # NB: We don't actually check for other message types
            # here; any message from the FE is treated as an
            # indication that it's making forward progress.
    finally:
        # Cancel the outstanding RPC to ensure any resources in use
        # are cleaned up; note that this is idempotent (in case the
        # RPC has already completed).
        await_rpc.cancel()

    # Delete task with timeout
    await fe_stub.delete_task(
        DeleteTaskRequest(task_id=task.task_id), timeout=_DELETE_TASK_TIMEOUT_SECS
    )

    if task_result is None:
        raise grpc.aio.AioRpcError(
            grpc.StatusCode.CANCELLED,
            None,
            None,
            "Function Executor didn't return function/task alloc result",
        )

    return task_result


def _task_output_from_function_executor_result(
    allocation: TaskAllocation,
    result: TaskResult,
    execution_start_time: Optional[float],
    execution_end_time: Optional[float],
    logger: Any,
) -> TaskOutput:
    response_validator = MessageValidator(result)
    response_validator.required_field("outcome_code")

    metrics = TaskMetrics(counters={}, timers={})
    if result.HasField("metrics"):
        # Can be None if e.g. function failed.
        metrics.counters = dict(result.metrics.counters)
        metrics.timers = dict(result.metrics.timers)

    outcome_code: TaskOutcomeCode = _to_task_outcome_code(
        result.outcome_code, logger=logger
    )
    failure_reason: Optional[TaskFailureReason] = None
    invocation_error_output: Optional[SerializedObjectInsideBLOB] = None
    uploaded_invocation_error_blob: Optional[BLOB] = None

    if outcome_code == TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE:
        response_validator.required_field("failure_reason")
        failure_reason: Optional[TaskFailureReason] = _to_task_failure_reason(
            result.failure_reason, logger
        )
        if failure_reason == TaskFailureReason.TASK_FAILURE_REASON_INVOCATION_ERROR:
            response_validator.required_field("invocation_error_output")
            response_validator.required_field("uploaded_invocation_error_blob")
            invocation_error_output = result.invocation_error_output
            uploaded_invocation_error_blob = result.uploaded_invocation_error_blob
    elif outcome_code == TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS:
        # function_outputs can have no items, this happens when the function returns None.
        response_validator.required_field("uploaded_function_outputs_blob")

    return TaskOutput(
        allocation=allocation,
        outcome_code=outcome_code,
        failure_reason=failure_reason,
        function_outputs=list(result.function_outputs),
        uploaded_function_outputs_blob=result.uploaded_function_outputs_blob,
        invocation_error_output=invocation_error_output,
        uploaded_invocation_error_blob=uploaded_invocation_error_blob,
        next_functions=list(result.next_functions),
        metrics=metrics,
        execution_start_time=execution_start_time,
        execution_end_time=execution_end_time,
    )


def _log_task_execution_finished(output: TaskOutput, logger: Any) -> None:
    logger.info(
        "finished running task",
        success=output.outcome_code == TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS,
        outcome_code=TaskOutcomeCode.Name(output.outcome_code),
        failure_reason=(
            TaskFailureReason.Name(output.failure_reason)
            if output.failure_reason is not None
            else None
        ),
    )


def _process_task_diagnostics(task_diagnostics: TaskDiagnostics, logger: Any) -> None:
    MessageValidator(task_diagnostics).required_field("function_executor_log")
    # Uncomment these lines once we stop printing FE logs to stdout/stderr.
    # Print FE logs directly to Executor logs so operators can see them.
    # logger.info("Function Executor logs during task execution:")
    # print(task_diagnostics.function_executor_log)


def _to_task_outcome_code(
    fe_task_outcome_code: FETaskOutcomeCode, logger
) -> TaskOutcomeCode:
    if fe_task_outcome_code == FETaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS:
        return TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS
    elif fe_task_outcome_code == FETaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE:
        return TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE
    else:
        logger.warning(
            "unknown TaskOutcomeCode received from Function Executor",
            value=FETaskOutcomeCode.Name(fe_task_outcome_code),
        )
        return TaskOutcomeCode.TASK_OUTCOME_CODE_UNKNOWN


def _to_task_failure_reason(
    fe_task_failure_reason: FETaskFailureReason, logger: Any
) -> TaskFailureReason:
    if fe_task_failure_reason == FETaskFailureReason.TASK_FAILURE_REASON_FUNCTION_ERROR:
        return TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_ERROR
    elif (
        fe_task_failure_reason
        == FETaskFailureReason.TASK_FAILURE_REASON_INVOCATION_ERROR
    ):
        return TaskFailureReason.TASK_FAILURE_REASON_INVOCATION_ERROR
    elif (
        fe_task_failure_reason == FETaskFailureReason.TASK_FAILURE_REASON_INTERNAL_ERROR
    ):
        return TaskFailureReason.TASK_FAILURE_REASON_INTERNAL_ERROR
    else:
        logger.warning(
            "unknown TaskFailureReason received from Function Executor",
            value=FETaskFailureReason.Name(fe_task_failure_reason),
        )
        return TaskFailureReason.TASK_FAILURE_REASON_UNKNOWN
