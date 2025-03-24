import asyncio
import time
from typing import Any, Optional

import grpc
from tensorlake.function_executor.proto.function_executor_pb2 import (
    RunTaskRequest,
    RunTaskResponse,
    SerializedObject,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.message_validator import MessageValidator

from indexify.proto.executor_api_pb2 import Task

from ..downloader import Downloader
from ..function_executor.function_executor_state import FunctionExecutorState
from ..function_executor.function_executor_status import FunctionExecutorStatus
from ..function_executor.metrics.single_task_runner import (
    metric_function_executor_run_task_rpc_errors,
    metric_function_executor_run_task_rpc_latency,
    metric_function_executor_run_task_rpcs,
)
from ..function_executor.task_output import TaskMetrics, TaskOutput

# TODO: combine these metrics into a single python file once gRPC migration is over and old code is removed.
from ..metrics.executor import (
    METRIC_TASKS_COMPLETED_OUTCOME_ALL,
    METRIC_TASKS_COMPLETED_OUTCOME_ERROR_CUSTOMER_CODE,
    METRIC_TASKS_COMPLETED_OUTCOME_ERROR_PLATFORM,
    METRIC_TASKS_COMPLETED_OUTCOME_SUCCESS,
    metric_task_completion_latency,
    metric_task_outcome_report_latency,
    metric_task_outcome_report_retries,
    metric_task_outcome_reports,
    metric_tasks_completed,
    metric_tasks_reporting_outcome,
)
from ..metrics.task_runner import (
    metric_task_policy_errors,
    metric_task_policy_latency,
    metric_task_policy_runs,
    metric_task_run_latency,
    metric_task_run_platform_errors,
    metric_task_runs,
    metric_tasks_blocked_by_policy,
    metric_tasks_blocked_by_policy_per_function_name,
    metric_tasks_running,
)
from ..task_reporter import TaskReporter
from .completed_tasks_container import CompletedTasksContainer

_TASK_OUTCOME_REPORT_BACKOFF_SEC = 5.0


class FunctionTimeoutError(Exception):
    """Exception raised when a customer's task execution exceeds the allowed timeout."""

    def __init__(self, message: str):
        super().__init__(message)


class TaskController:
    def __init__(
        self,
        task: Task,
        function_executor_state: FunctionExecutorState,
        downloader: Downloader,
        task_reporter: TaskReporter,
        completed_tasks_container: CompletedTasksContainer,
        logger: Any,
    ):
        """Creates a new TaskController instance.

        Raises ValueError if the supplied Task is not valid.
        """
        _validate_task(task)
        self._task: Task = task
        self._function_executor_state: FunctionExecutorState = function_executor_state
        self._downloader: Downloader = downloader
        self._task_reporter: TaskReporter = task_reporter
        self._completed_tasks_container: CompletedTasksContainer = (
            completed_tasks_container
        )
        self._logger: Any = logger.bind(
            function_executor_id=function_executor_state.id,
            task_id=task.id,
            module=__name__,
            namespace=task.namespace,
            graph_name=task.graph_name,
            graph_version=task.graph_version,
            function_name=task.function_name,
            invocation_id=task.graph_invocation_id,
        )
        self._is_running: bool = False
        self._is_cancelled: bool = False
        self._input: Optional[SerializedObject] = None
        self._init_value: Optional[SerializedObject] = None
        self._output: Optional[TaskOutput] = None

    async def cancel_task(self) -> None:
        """Cancells the task."""
        self._is_cancelled = True

        async with self._function_executor_state.lock:
            if not self._is_running:
                return

            # Mark the Function Executor as unhealthy to destroy it to cancel the running function.
            # If FE status changed, then it means that we're off normal task execution path, e.g.
            # Server decided to do something with FE.
            if (
                self._function_executor_state.status
                == FunctionExecutorStatus.RUNNING_TASK
            ):
                # TODO: Add a separate FE status for cancelled function so we don't lie to server that FE is unhealthy to destroy it.
                await self._function_executor_state.set_status(
                    FunctionExecutorStatus.UNHEALTHY,
                )
                self._logger.warning("task is cancelled")
            else:
                self._logger.warning(
                    "skipping marking Function Executor unhealthy on task cancellation due to unexpected FE status",
                    status=self._function_executor_state.status.name,
                )

    async def run_task(self) -> None:
        """Runs the supplied task and does full managemenet of its lifecycle.

        Doesn't raise any exceptions."""
        start_time: float = time.monotonic()

        try:
            # The task can be cancelled at any time but we'll just wait until FE gets shutdown
            # because we require this to happen from the cancel_task() caller.
            self._input = await self._downloader.download_input(
                namespace=self._task.namespace,
                graph_name=self._task.graph_name,
                graph_invocation_id=self._task.graph_invocation_id,
                input_key=self._task.input_key,
                logger=self._logger,
            )
            if self._task.HasField("reducer_output_key"):
                self._init_value = await self._downloader.download_init_value(
                    namespace=self._task.namespace,
                    graph_name=self._task.graph_name,
                    function_name=self._task.function_name,
                    graph_invocation_id=self._task.graph_invocation_id,
                    reducer_output_key=self._task.reducer_output_key,
                    logger=self._logger,
                )

            await self._wait_for_idle_function_executor()

            with (
                metric_task_run_platform_errors.count_exceptions(),
                metric_tasks_running.track_inprogress(),
                metric_task_run_latency.time(),
            ):
                metric_task_runs.inc()
                await self._run_task()

            self._logger.info("task execution finished", success=self._output.success)
        except FunctionTimeoutError:
            self._output = TaskOutput.function_timeout(
                task_id=self._task.id,
                namespace=self._task.namespace,
                graph_name=self._task.graph_name,
                function_name=self._task.function_name,
                graph_version=self._task.graph_version,
                graph_invocation_id=self._task.graph_invocation_id,
            )
            async with self._function_executor_state.lock:
                # Mark the Function Executor as unhealthy to destroy it to cancel the running function.
                # If FE status changed, then it means that we're off normal task execution path, e.g.
                # Server decided to do something with FE.
                if (
                    self._function_executor_state.status
                    == FunctionExecutorStatus.RUNNING_TASK
                ):
                    # TODO: Add a separate FE status for timed out function so we don't lie to server that FE is unhealthy to destroy it.
                    await self._function_executor_state.set_status(
                        FunctionExecutorStatus.UNHEALTHY,
                    )
                else:
                    self._logger.warning(
                        "skipping marking Function Executor unhealthy on task timeout due to unexpected FE status",
                        status=self._function_executor_state.status.name,
                    )
        except Exception as e:
            self._output = TaskOutput.internal_error(
                task_id=self._task.id,
                namespace=self._task.namespace,
                graph_name=self._task.graph_name,
                function_name=self._task.function_name,
                graph_version=self._task.graph_version,
                graph_invocation_id=self._task.graph_invocation_id,
            )
            self._logger.error("task execution failed", exc_info=e)
        finally:
            # Release the Function Executor so others can run tasks on it if FE status didn't change.
            # If FE status changed, then it means that we're off normal task execution path, e.g.
            # Server decided to do something with FE.
            async with self._function_executor_state.lock:
                if (
                    self._function_executor_state.status
                    == FunctionExecutorStatus.RUNNING_TASK
                ):
                    await self._function_executor_state.set_status(
                        FunctionExecutorStatus.IDLE
                    )
                else:
                    self._logger.warning(
                        "skipping marking Function Executor IDLE due to unexpected FE status",
                        status=self._function_executor_state.status,
                    )

        _log_function_metrics(self._output, self._logger)

        with (
            metric_tasks_reporting_outcome.track_inprogress(),
            metric_task_outcome_report_latency.time(),
        ):
            metric_task_outcome_reports.inc()
            await self._report_task_outcome()

        metric_task_completion_latency.observe(time.monotonic() - start_time)

    async def _wait_for_idle_function_executor(self) -> None:
        """Waits until the Function Executor is in IDLE state.

        Raises an Exception if the Function Executor is in SHUTDOWN state.
        """
        with (
            metric_task_policy_errors.count_exceptions(),
            metric_tasks_blocked_by_policy.track_inprogress(),
            metric_tasks_blocked_by_policy_per_function_name.labels(
                function_name=self._task.function_name
            ).track_inprogress(),
            metric_task_policy_latency.time(),
        ):
            metric_task_policy_runs.inc()
            self._logger.info(
                "task is blocked by policy: waiting for idle function executor"
            )
            async with self._function_executor_state.lock:
                await self._function_executor_state.wait_status(
                    allowlist=[
                        FunctionExecutorStatus.IDLE,
                        FunctionExecutorStatus.SHUTDOWN,
                    ]
                )
                if (
                    self._function_executor_state.status
                    == FunctionExecutorStatus.SHUTDOWN
                ):
                    raise Exception(
                        "Task's Function Executor got shutdown, can't run task"
                    )
                await self._function_executor_state.set_status(
                    FunctionExecutorStatus.RUNNING_TASK
                )

            # At this point the Function Executor belongs to this task controller due to RUNNING_TASK status.
            # We can now unlock the FE state. We have to update the FE status once the task succeeds or fails.

    async def _run_task(self) -> None:
        request: RunTaskRequest = RunTaskRequest(
            namespace=self._task.namespace,
            graph_name=self._task.graph_name,
            graph_version=self._task.graph_version,
            function_name=self._task.function_name,
            graph_invocation_id=self._task.graph_invocation_id,
            task_id=self._task.id,
            function_input=self._input,
        )
        if self._init_value is not None:
            request.function_init_value.CopyFrom(self._init_value)
        channel: grpc.aio.Channel = (
            self._function_executor_state.function_executor.channel()
        )

        timeout_sec: Optional[float] = None
        if self._task.HasField("timeout_ms"):
            # TODO: Add integration tests with function timeout when end-to-end implementation is done.
            timeout_sec = self._task.timeout_ms / 1000.0

        async with _RunningTaskContextManager(
            task=self._task,
            function_executor_state=self._function_executor_state,
        ):
            with (
                metric_function_executor_run_task_rpc_errors.count_exceptions(),
                metric_function_executor_run_task_rpc_latency.time(),
            ):
                metric_function_executor_run_task_rpcs.inc()
                # If this RPC failed due to customer code crashing the server we won't be
                # able to detect this. We'll treat this as our own error for now and thus
                # let the AioRpcError to be raised here.
                try:
                    response: RunTaskResponse = await FunctionExecutorStub(
                        channel
                    ).run_task(request, timeout=timeout_sec)
                except grpc.aio.AioRpcError as e:
                    if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                        raise FunctionTimeoutError(
                            f"Task execution timeout {timeout_sec} expired"
                        ) from e
                    raise

        self._output = _task_output(task=self._task, response=response)

    async def _report_task_outcome(self) -> None:
        """Reports the task with the given output to the server.

        Doesn't raise any Exceptions. Runs till the reporting is successful."""
        reporting_retries: int = 0

        while True:
            logger = self._logger.bind(retries=reporting_retries)
            if self._is_cancelled:
                logger.warning(
                    "task is cancelled, skipping its outcome reporting to workaround lack of server side retries"
                )
                break

            try:
                await self._task_reporter.report(output=self._output, logger=logger)
                break
            except Exception as e:
                logger.error(
                    "failed to report task",
                    exc_info=e,
                )
                reporting_retries += 1
                metric_task_outcome_report_retries.inc()
                await asyncio.sleep(_TASK_OUTCOME_REPORT_BACKOFF_SEC)

        await self._completed_tasks_container.add(self._task.id)
        metric_tasks_completed.labels(outcome=METRIC_TASKS_COMPLETED_OUTCOME_ALL).inc()
        if self._output.is_internal_error:
            metric_tasks_completed.labels(
                outcome=METRIC_TASKS_COMPLETED_OUTCOME_ERROR_PLATFORM
            ).inc()
        elif self._output.success:
            metric_tasks_completed.labels(
                outcome=METRIC_TASKS_COMPLETED_OUTCOME_SUCCESS
            ).inc()
        else:
            metric_tasks_completed.labels(
                outcome=METRIC_TASKS_COMPLETED_OUTCOME_ERROR_CUSTOMER_CODE
            ).inc()


def _validate_task(task: Task) -> None:
    """Validates the supplied Task.

    Raises ValueError if the Task is not valid.
    """
    validator = MessageValidator(task)
    validator.required_field("id")
    validator.required_field("namespace")
    validator.required_field("graph_name")
    validator.required_field("graph_version")
    validator.required_field("function_name")
    validator.required_field("graph_invocation_id")
    validator.required_field("input_key")


def _task_output(task: Task, response: RunTaskResponse) -> TaskOutput:
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
        task_id=task.id,
        namespace=task.namespace,
        graph_name=task.graph_name,
        function_name=task.function_name,
        graph_version=task.graph_version,
        graph_invocation_id=task.graph_invocation_id,
        stdout=response.stdout,
        stderr=response.stderr,
        reducer=response.is_reducer,
        success=response.success,
        metrics=metrics,
    )

    if response.HasField("function_output"):
        output.function_output = response.function_output
    if response.HasField("router_output"):
        output.router_output = response.router_output

    return output


# Temporary workaround is logging customer metrics until we store them somewhere
# for future retrieval and processing.
def _log_function_metrics(output: TaskOutput, logger: Any):
    if output.metrics is None:
        return

    logger = logger.bind(
        invocation_id=output.graph_invocation_id,
        function_name=output.function_name,
        graph_name=output.graph_name,
        namespace=output.namespace,
    )

    for counter_name, counter_value in output.metrics.counters.items():
        logger.info(
            "function_metric", counter_name=counter_name, counter_value=counter_value
        )
    for timer_name, timer_value in output.metrics.timers.items():
        logger.info("function_metric", timer_name=timer_name, timer_value=timer_value)


class _RunningTaskContextManager:
    """Performs all the actions required before and after running a task."""

    def __init__(
        self,
        task_controller: TaskController,
    ):
        self._task_controller: TaskController = task_controller

    async def __aenter__(self):
        self._task_controller._function_executor_state.function_executor.invocation_state_client().add_task_to_invocation_id_entry(
            task_id=self._task_controller._task.id,
            invocation_id=self._task_controller._task.graph_invocation_id,
        )
        self._task_controller._is_running = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._task_controller._is_running = False
        self._task_controller._function_executor_state.function_executor.invocation_state_client().remove_task_to_invocation_id_entry(
            task_id=self._task_controller._task.id,
        )
