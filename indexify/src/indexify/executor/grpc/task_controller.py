import asyncio
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
from ..function_executor.function_executor import FunctionExecutor
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
    metric_tasks_fetched,
    metric_tasks_reporting_outcome,
)
from ..metrics.task_runner import (
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
from .metrics.task_controller import metric_task_cancellations

_TASK_OUTCOME_REPORT_BACKOFF_SEC = 5.0


def validate_task(task: Task) -> None:
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
    if not (task.HasField("input_key") or task.HasField("input")):
        raise ValueError(
            "Task must have either input_key or input field set. " f"Got task: {task}"
        )


def task_logger(task: Task, logger: Any) -> Any:
    """Returns a logger bound with the task's metadata.

    The function assumes that the task might be invalid."""
    return logger.bind(
        task_id=task.id if task.HasField("id") else None,
        namespace=task.namespace if task.HasField("namespace") else None,
        graph_name=task.graph_name if task.HasField("graph_name") else None,
        graph_version=task.graph_version if task.HasField("graph_version") else None,
        function_name=task.function_name if task.HasField("function_name") else None,
        graph_invocation_id=(
            task.graph_invocation_id if task.HasField("graph_invocation_id") else None
        ),
    )


class TaskController:
    def __init__(
        self,
        task: Task,
        downloader: Downloader,
        task_reporter: TaskReporter,
        function_executor_id: str,
        function_executor_state: FunctionExecutorState,
        logger: Any,
    ):
        """Creates a new TaskController instance.

        The supplied Task must be already validated by the caller using validate_task().
        """
        self._task: Task = task
        self._downloader: Downloader = downloader
        self._task_reporter: TaskReporter = task_reporter
        self._function_executor_id: str = function_executor_id
        self._function_executor_state: FunctionExecutorState = function_executor_state
        self._logger: Any = task_logger(task, logger).bind(
            function_executor_id=function_executor_id,
            module=__name__,
        )

        self._input: Optional[SerializedObject] = None
        self._init_value: Optional[SerializedObject] = None
        self._is_timed_out: bool = False
        # Automatically start the controller on creation.
        self._task_runner: asyncio.Task = asyncio.create_task(
            self._run(), name="task controller task runner"
        )

    def function_executor_id(self) -> str:
        return self._function_executor_id

    def task(self) -> Task:
        return self._task

    async def destroy(self) -> None:
        """Destroys the controller and cancells the task if it didn't finish yet.

        A running task is cancelled by destroying its Function Executor.
        Doesn't raise any exceptions.
        """
        if self._task_runner.done():
            return  # Nothin to do, the task is finished already.

        # The task runner code handles asyncio.CancelledError properly.
        self._task_runner.cancel()
        # Don't await the cancelled task to not block the caller unnecessary.

    async def _run(self) -> None:
        metric_tasks_fetched.inc()
        with metric_task_completion_latency.time():
            await self._run_task()

    async def _run_task(self) -> None:
        """Runs the supplied task and does full managemenet of its lifecycle.

        Doesn't raise any exceptions."""
        output: Optional[TaskOutput] = None

        try:
            await self._download_inputs()
            output = await self._run_task_when_function_executor_is_available()
            self._logger.info("task execution finished", success=output.success)
            _log_function_metrics(output, self._logger)
        except Exception as e:
            metric_task_run_platform_errors.inc(),
            output = self._internal_error_output()
            self._logger.error("task execution failed", exc_info=e)
        except asyncio.CancelledError:
            metric_task_cancellations.inc()
            self._logger.info("task execution cancelled")
            # Don't report task outcome according to the current policy.
            # asyncio.CancelledError can't be suppressed, see Python docs.
            raise

        # Current task outcome reporting policy:
        # Don't report task outcomes for tasks that didn't fail with internal or customer error.
        # This is required to simplify the protocol so Server doesn't need to care about task states
        # and cancel each tasks carefully to not get its outcome as failed.
        with (
            metric_tasks_reporting_outcome.track_inprogress(),
            metric_task_outcome_report_latency.time(),
        ):
            metric_task_outcome_reports.inc()
            await self._report_task_outcome(output)

    async def _download_inputs(self) -> None:
        """Downloads the task inputs and init value.

        Raises an Exception if the inputs failed to download.
        """
        self._input = await self._downloader.download_input(
            namespace=self._task.namespace,
            graph_name=self._task.graph_name,
            graph_invocation_id=self._task.graph_invocation_id,
            input_key=self._task.input_key,
            data_payload=self._task.input if self._task.HasField("input") else None,
            logger=self._logger,
        )

        if self._task.HasField("reducer_output_key") or self._task.HasField(
            "reducer_input"
        ):
            self._init_value = await self._downloader.download_init_value(
                namespace=self._task.namespace,
                graph_name=self._task.graph_name,
                function_name=self._task.function_name,
                graph_invocation_id=self._task.graph_invocation_id,
                reducer_output_key=(
                    self._task.reducer_output_key
                    if self._task.HasField("reducer_output_key")
                    else ""
                ),
                data_payload=(
                    self._task.reducer_input
                    if self._task.HasField("reducer_input")
                    else None
                ),
                logger=self._logger,
            )

    async def _run_task_when_function_executor_is_available(self) -> TaskOutput:
        """Runs the task on the Function Executor when it's available.

        Raises an Exception if task failed due to an internal error."""
        await self._acquire_function_executor()

        next_status: FunctionExecutorStatus = FunctionExecutorStatus.IDLE
        try:
            return await self._run_task_on_acquired_function_executor()
        except asyncio.CancelledError:
            # This one is raised here when destroy() was called while we were running the task on this FE.
            next_status = FunctionExecutorStatus.UNHEALTHY
            # asyncio.CancelledError can't be suppressed, see Python docs.
            raise
        finally:
            # If the task finished running on FE then put it into IDLE state so other tasks can run on it.
            # Otherwise, mark the FE as unhealthy to force its destruction so the task stops running on it eventually
            # and no other tasks run on this FE because it'd result in undefined behavior.
            if self._is_timed_out:
                next_status = FunctionExecutorStatus.UNHEALTHY
            await self._release_function_executor(next_status=next_status)

    async def _acquire_function_executor(self) -> None:
        """Waits until the Function Executor is in IDLE state and then locks it so the task can run on it.

        Doesn't raise any exceptions.
        """
        with (
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
                    allowlist=[FunctionExecutorStatus.IDLE]
                )
                await self._function_executor_state.set_status(
                    FunctionExecutorStatus.RUNNING_TASK
                )

            # At this point the Function Executor belongs to this task controller due to RUNNING_TASK status.
            # We can now unlock the FE state. We have to update the FE status once the task succeeds or fails.

    async def _release_function_executor(
        self, next_status: FunctionExecutorStatus
    ) -> None:
        # Release the Function Executor so others can run tasks on it if FE status didn't change.
        # If FE status changed, then it means that we're off normal task execution path, e.g.
        # Server decided to do something with FE.
        async with self._function_executor_state.lock:
            if (
                self._function_executor_state.status
                == FunctionExecutorStatus.RUNNING_TASK
            ):
                await self._function_executor_state.set_status(next_status)
                if next_status == FunctionExecutorStatus.UNHEALTHY:
                    # Destroy the unhealthy FE asap so it doesn't consume resources.
                    # Don't do it under the state lock to not add unnecessary delays.
                    asyncio.create_task(
                        self._function_executor_state.function_executor.destroy()
                    )
                    self._function_executor_state.function_executor = None
            else:
                self._logger.warning(
                    "skipping releasing Function Executor after running the task due to unexpected Function Executor status",
                    status=self._function_executor_state.status.name,
                    next_status=next_status.name,
                )

    async def _run_task_on_acquired_function_executor(self) -> TaskOutput:
        """Runs the task on the Function Executor acquired by this task already and returns the output.

        Raises an Exception if the task failed to run due to an internal error."""
        with metric_tasks_running.track_inprogress(), metric_task_run_latency.time():
            metric_task_runs.inc()
            return await self._run_task_rpc_on_function_executor()

    async def _run_task_rpc_on_function_executor(self) -> TaskOutput:
        """Runs the task on the Function Executor and returns the output.

        Raises an Exception if the task failed to run due to an internal error.
        """
        request: RunTaskRequest = RunTaskRequest(
            namespace=self._task.namespace,
            graph_name=self._task.graph_name,
            graph_version=self._task.graph_version,
            function_name=self._task.function_name,
            graph_invocation_id=self._task.graph_invocation_id,
            task_id=self._task.id,
            function_input=self._input,
        )
        # Don't keep the input in memory after we started running the task.
        self._input = None

        if self._init_value is not None:
            request.function_init_value.CopyFrom(self._init_value)
            # Don't keep the init value in memory after we started running the task.
            self._init_value = None

        channel: grpc.aio.Channel = (
            self._function_executor_state.function_executor.channel()
        )

        timeout_sec: Optional[float] = None
        if self._task.HasField("timeout_ms"):
            # TODO: Add integration tests with function timeout when end-to-end implementation is done.
            timeout_sec = self._task.timeout_ms / 1000.0

        async with _RunningTaskContextManager(
            task=self._task,
            function_executor=self._function_executor_state.function_executor,
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
                        # Not logging customer error.
                        self._is_timed_out = True
                        return self._function_timeout_output(timeout_sec=timeout_sec)
                    raise

        return _task_output_from_function_executor_response(
            task=self._task, response=response
        )

    async def _report_task_outcome(self, output: TaskOutput) -> None:
        """Reports the task with the given output to the server.

        Doesn't raise any Exceptions. Runs till the reporting is successful."""
        reporting_retries: int = 0

        while True:
            logger = self._logger.bind(retries=reporting_retries)
            try:
                await self._task_reporter.report(output=output, logger=logger)
                break
            except Exception as e:
                logger.error(
                    "failed to report task",
                    exc_info=e,
                )
                reporting_retries += 1
                metric_task_outcome_report_retries.inc()
                await asyncio.sleep(_TASK_OUTCOME_REPORT_BACKOFF_SEC)

        metric_tasks_completed.labels(outcome=METRIC_TASKS_COMPLETED_OUTCOME_ALL).inc()
        if output.is_internal_error:
            metric_tasks_completed.labels(
                outcome=METRIC_TASKS_COMPLETED_OUTCOME_ERROR_PLATFORM
            ).inc()
        elif output.success:
            metric_tasks_completed.labels(
                outcome=METRIC_TASKS_COMPLETED_OUTCOME_SUCCESS
            ).inc()
        else:
            metric_tasks_completed.labels(
                outcome=METRIC_TASKS_COMPLETED_OUTCOME_ERROR_CUSTOMER_CODE
            ).inc()

    def _internal_error_output(self) -> TaskOutput:
        return TaskOutput.internal_error(
            task_id=self._task.id,
            namespace=self._task.namespace,
            graph_name=self._task.graph_name,
            function_name=self._task.function_name,
            graph_version=self._task.graph_version,
            graph_invocation_id=self._task.graph_invocation_id,
            output_payload_uri_prefix=(
                self._task.output_payload_uri_prefix
                if self._task.HasField("output_payload_uri_prefix")
                else None
            ),
        )

    def _function_timeout_output(self, timeout_sec: float) -> TaskOutput:
        return TaskOutput.function_timeout(
            task_id=self._task.id,
            namespace=self._task.namespace,
            graph_name=self._task.graph_name,
            function_name=self._task.function_name,
            graph_version=self._task.graph_version,
            graph_invocation_id=self._task.graph_invocation_id,
            timeout_sec=timeout_sec,
            output_payload_uri_prefix=(
                self._task.output_payload_uri_prefix
                if self._task.HasField("output_payload_uri_prefix")
                else None
            ),
        )


def _task_output_from_function_executor_response(
    task: Task, response: RunTaskResponse
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
        output_payload_uri_prefix=(
            task.output_payload_uri_prefix
            if task.HasField("output_payload_uri_prefix")
            else None
        ),
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
        task: Task,
        function_executor: FunctionExecutor,
    ):
        self._task = task
        self._function_executor: FunctionExecutor = function_executor

    async def __aenter__(self):
        self._function_executor.invocation_state_client().add_task_to_invocation_id_entry(
            task_id=self._task.id,
            invocation_id=self._task.graph_invocation_id,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._function_executor.invocation_state_client().remove_task_to_invocation_id_entry(
            task_id=self._task.id,
        )
