import asyncio
import time
from typing import Any, Optional

import grpc
from tensorlake.function_executor.proto.function_executor_pb2 import (
    RunTaskRequest,
    RunTaskResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.message_validator import MessageValidator

from indexify.executor.function_executor.function_executor import FunctionExecutor
from indexify.executor.function_executor.health_checker import HealthCheckResult
from indexify.proto.executor_api_pb2 import (
    FunctionExecutorTerminationReason,
    Task,
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


async def run_task_on_function_executor(
    task_info: TaskInfo, function_executor: FunctionExecutor, logger: Any
) -> TaskExecutionFinished:
    """Runs the task on the Function Executor and sets task_info.output with the result.

    Doesn't raise any exceptions.
    """
    logger = logger.bind(module=__name__)
    request: RunTaskRequest = RunTaskRequest(
        namespace=task_info.task.namespace,
        graph_name=task_info.task.graph_name,
        graph_version=task_info.task.graph_version,
        function_name=task_info.task.function_name,
        graph_invocation_id=task_info.task.graph_invocation_id,
        task_id=task_info.task.id,
        function_input=task_info.input,
    )
    # Don't keep the input in memory after we started running the task.
    task_info.input = None

    if task_info.init_value is not None:
        request.function_init_value.CopyFrom(task_info.init_value)
        # Don't keep the init value in memory after we started running the task.
        task_info.init_value = None

    function_executor.invocation_state_client().add_task_to_invocation_id_entry(
        task_id=task_info.task.id,
        invocation_id=task_info.task.graph_invocation_id,
    )

    metric_function_executor_run_task_rpcs.inc()
    metric_function_executor_run_task_rpcs_in_progress.inc()
    start_time = time.monotonic()
    # Not None if the Function Executor should be terminated after running the task.
    function_executor_termination_reason: Optional[
        FunctionExecutorTerminationReason
    ] = None

    # If this RPC failed due to customer code crashing the server we won't be
    # able to detect this. We'll treat this as our own error for now and thus
    # let the AioRpcError to be raised here.
    timeout_sec = task_info.task.timeout_ms / 1000.0
    try:
        channel: grpc.aio.Channel = function_executor.channel()
        response: RunTaskResponse = await FunctionExecutorStub(channel).run_task(
            request, timeout=timeout_sec
        )
        task_info.output = _task_output_from_function_executor_response(
            task=task_info.task,
            response=response,
            allocation_id=task_info.allocation_id,
        )
    except grpc.aio.AioRpcError as e:
        if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            # The task is still running in FE, we only cancelled the client-side RPC.
            function_executor_termination_reason = (
                FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_TIMEOUT
            )
            task_info.output = TaskOutput.function_timeout(
                task=task_info.task,
                allocation_id=task_info.allocation_id,
                timeout_sec=timeout_sec,
            )
        else:
            metric_function_executor_run_task_rpc_errors.inc()
            logger.error("task execution failed", exc_info=e)
            task_info.output = TaskOutput.internal_error(
                task=task_info.task, allocation_id=task_info.allocation_id
            )
    except asyncio.CancelledError:
        # The task is still running in FE, we only cancelled the client-side RPC.
        function_executor_termination_reason = (
            FunctionExecutorTerminationReason.FUNCTION_EXECUTOR_TERMINATION_REASON_FUNCTION_CANCELLED
        )
        task_info.output = TaskOutput.task_cancelled(
            task=task_info.task, allocation_id=task_info.allocation_id
        )
    except Exception as e:
        metric_function_executor_run_task_rpc_errors.inc()
        logger.error("task execution failed", exc_info=e)
        task_info.output = TaskOutput.internal_error(
            task=task_info.task, allocation_id=task_info.allocation_id
        )

    metric_function_executor_run_task_rpc_latency.observe(time.monotonic() - start_time)
    metric_function_executor_run_task_rpcs_in_progress.dec()

    function_executor.invocation_state_client().remove_task_to_invocation_id_entry(
        task_id=task_info.task.id,
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


def _task_output_from_function_executor_response(
    task: Task, response: RunTaskResponse, allocation_id: str
) -> TaskOutput:
    response_validator = MessageValidator(response)
    response_validator.required_field("stdout")
    response_validator.required_field("stderr")
    response_validator.required_field("is_reducer")
    response_validator.required_field("success")

    metrics = TaskMetrics(counters={}, timers={})
    if response.HasField("metrics"):
        # Can be None if e.g. function failed.
        metrics.counters = dict(response.metrics.counters)
        metrics.timers = dict(response.metrics.timers)

    output = TaskOutput(
        task=task,
        allocation_id=allocation_id,
        outcome_code=(
            TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS
            if response.success
            else TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE
        ),
        failure_reason=(
            None
            if response.success
            else TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_ERROR
        ),
        stdout=response.stdout,
        stderr=response.stderr,
        reducer=response.is_reducer,
        metrics=metrics,
    )

    if response.HasField("function_output"):
        output.function_output = response.function_output
    if response.HasField("router_output"):
        output.router_output = response.router_output

    return output


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
