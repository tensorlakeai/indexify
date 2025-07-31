import asyncio
import os
import random
import time
from typing import Any, Optional

import grpc
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AwaitTaskProgress,
    AwaitTaskRequest,
    CreateTaskRequest,
    DeleteTaskRequest,
    FunctionInputs,
    SerializedObject,
    Task,
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

_ENABLE_INJECT_TASK_CANCELLATIONS = (
    os.getenv("INDEXIFY_INJECT_TASK_CANCELLATIONS", "0") == "1"
)

_CREATE_TASK_TIMEOUT_SECS = 5
_DELETE_TASK_TIMEOUT_SECS = 5


async def run_task_on_function_executor(
    task_info: TaskInfo, function_executor: FunctionExecutor, logger: Any
) -> TaskExecutionFinished:
    """Runs the task on the Function Executor and sets task_info.output with the result.

    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)
    task = Task(
        task_id=task_info.allocation.task.id,
        namespace=task_info.allocation.task.namespace,
        graph_name=task_info.allocation.task.graph_name,
        graph_version=task_info.allocation.task.graph_version,
        function_name=task_info.allocation.task.function_name,
        graph_invocation_id=task_info.allocation.task.graph_invocation_id,
        allocation_id=task_info.allocation.allocation_id,
        request=FunctionInputs(function_input=task_info.input),
    )
    # Don't keep the input in memory after we started running the task.
    task_info.input = None

    if task_info.init_value is not None:
        task.request.function_init_value.CopyFrom(task_info.init_value)
        # Don't keep the init value in memory after we started running the task.
        task_info.init_value = None

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
    timeout_sec = task_info.allocation.task.timeout_ms / 1000.0
    try:
        task_result = await _run_task_rpcs(task, function_executor, timeout_sec)

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
            timeout_sec=timeout_sec,
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
            "Unexpected internal error during task lifecycle RPC sequence", exc_info=e
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

    _log_task_execution_finished(output=task_info.output, logger=logger)

    return TaskExecutionFinished(
        task_info=task_info,
        function_executor_termination_reason=function_executor_termination_reason,
    )


async def _run_task_rpcs(
    task: Task, function_executor: FunctionExecutor, timeout_sec: float
) -> TaskResult:
    """Runs the task, returning the result, reporting errors via exceptions."""

    response: AwaitTaskProgress
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
            response = await asyncio.wait_for(await_rpc.read(), timeout=timeout_sec)
            if response.WhichOneof("response") == "task_result":
                # We're done waiting.
                break

            # NB: We don't actually check for other message types
            # here; any message from the FE is treated as an
            # indication that it's making forward progress.

            if response == grpc.aio.EOF:
                # Protocol error: we should get a task_result before
                # we see the RPC complete.
                raise grpc.aio.AioRpcError(
                    grpc.StatusCode.CANCELLED,
                    None,
                    None,
                    "Function Executor didn't return function/task alloc response",
                )
    finally:
        # Cancel the outstanding RPC to ensure any resources in use
        # are cleaned up; note that this is idempotent (in case the
        # RPC has already completed).
        await_rpc.cancel()

    # Delete task with timeout
    await fe_stub.delete_task(
        DeleteTaskRequest(task_id=task.task_id), timeout=_DELETE_TASK_TIMEOUT_SECS
    )

    return response.task_result


def _task_output_from_function_executor_result(
    allocation: TaskAllocation,
    result: TaskResult,
    execution_start_time: Optional[float],
    execution_end_time: Optional[float],
    logger: Any,
) -> TaskOutput:
    response_validator = MessageValidator(result)
    response_validator.required_field("stdout")
    response_validator.required_field("stderr")
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
    invocation_error_output: Optional[SerializedObject] = None

    if outcome_code == TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE:
        response_validator.required_field("failure_reason")
        failure_reason: Optional[TaskFailureReason] = _to_task_failure_reason(
            result.failure_reason, logger
        )
        if failure_reason == TaskFailureReason.TASK_FAILURE_REASON_INVOCATION_ERROR:
            response_validator.required_field("invocation_error_output")
            invocation_error_output = result.invocation_error_output

    if _ENABLE_INJECT_TASK_CANCELLATIONS:
        logger.warning("injecting cancellation failure for the task allocation")
        if (
            random.random() < 0.5
        ):  # 50% chance to get stable reproduction in manual testing
            outcome_code = TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE
            failure_reason = TaskFailureReason.TASK_FAILURE_REASON_TASK_CANCELLED

    return TaskOutput(
        allocation=allocation,
        outcome_code=outcome_code,
        failure_reason=failure_reason,
        invocation_error_output=invocation_error_output,
        function_outputs=result.function_outputs,
        next_functions=result.next_functions,
        stdout=result.stdout,
        stderr=result.stderr,
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


def _to_task_outcome_code(
    fe_task_outcome_code: FETaskOutcomeCode, logger
) -> TaskOutcomeCode:
    if fe_task_outcome_code == FETaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS:
        return TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS
    elif fe_task_outcome_code == FETaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE:
        return TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE
    else:
        logger.warning(
            "Unknown TaskOutcomeCode received from Function Executor",
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
            "Unknown TaskFailureReason received from Function Executor",
            value=FETaskFailureReason.Name(fe_task_failure_reason),
        )
        return TaskFailureReason.TASK_FAILURE_REASON_UNKNOWN
