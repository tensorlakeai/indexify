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

from indexify.proto.executor_api_pb2 import Task, TaskFailureReason, TaskOutcomeCode

from ..downloader import Downloader
from ..function_executor.function_executor import FunctionExecutor
from ..function_executor.function_executor_state import FunctionExecutorState
from ..function_executor.function_executor_status import FunctionExecutorStatus
from ..function_executor.health_checker import HealthCheckResult
from ..function_executor.metrics.single_task_runner import (
    metric_function_executor_run_task_rpc_errors,
    metric_function_executor_run_task_rpc_latency,
    metric_function_executor_run_task_rpcs,
)
from ..function_executor.task_output import TaskMetrics, TaskOutput
from .metrics.task_controller import (
    METRIC_TASKS_COMPLETED_FAILURE_REASON_ALL,
    METRIC_TASKS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR,
    METRIC_TASKS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
    METRIC_TASKS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR,
    METRIC_TASKS_COMPLETED_FAILURE_REASON_NONE,
    METRIC_TASKS_COMPLETED_OUTCOME_CODE_ALL,
    METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
    METRIC_TASKS_COMPLETED_OUTCOME_CODE_SUCCESS,
    metric_task_cancellations,
    metric_task_completion_latency,
    metric_task_output_upload_latency,
    metric_task_output_upload_retries,
    metric_task_output_uploads,
    metric_task_policy_latency,
    metric_task_policy_runs,
    metric_task_run_latency,
    metric_task_run_platform_errors,
    metric_task_runs,
    metric_tasks_blocked_by_policy,
    metric_tasks_blocked_by_policy_per_function_name,
    metric_tasks_completed,
    metric_tasks_fetched,
    metric_tasks_running,
    metric_tasks_uploading_outputs,
)
from .state_reporter import ExecutorStateReporter
from .task_output_uploader import TaskOutputUploader

_TASK_OUTPUT_UPLOAD_BACKOFF_SEC = 5.0


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
    validator.required_field("timeout_ms")
    validator.required_field("input")
    validator.required_field("output_payload_uri_prefix")
    validator.required_field("retry_policy")


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
        task_output_uploader: TaskOutputUploader,
        function_executor_id: str,
        function_executor_state: FunctionExecutorState,
        state_reporter: ExecutorStateReporter,
        logger: Any,
    ):
        """Creates a new TaskController instance.

        The supplied Task must be already validated by the caller using validate_task().
        """
        self._task: Task = task
        self._downloader: Downloader = downloader
        self._task_output_uploader: TaskOutputUploader = task_output_uploader
        self._function_executor_id: str = function_executor_id
        self._function_executor_state: FunctionExecutorState = function_executor_state
        self._state_reporter: ExecutorStateReporter = state_reporter
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
            self._logger.info(
                "task execution finished",
                success=output.outcome_code
                == TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS,
                outcome_code=TaskOutcomeCode.Name(output.outcome_code),
                failure_reason=(
                    TaskFailureReason.Name(output.failure_reason)
                    if output.failure_reason is not None
                    else None
                ),
            )
            _log_function_metrics(output, self._logger)
        except Exception as e:
            metric_task_run_platform_errors.inc(),
            output = self._internal_error_output()
            self._logger.error("task execution failed", exc_info=e)
        except asyncio.CancelledError:
            metric_task_cancellations.inc()
            # Suppress current asyncio task cancellation to report the task outcome and metrics.
            if hasattr(asyncio.current_task(), "uncancel"):
                # In Python 3.11+ we need to call uncancel() see:
                # https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.uncancel
                asyncio.current_task().uncancel()
            output = self._task_cancelled_output()
            self._logger.info("task execution cancelled")
        # Current task outcome reporting policy:
        # Don't report task outcomes for tasks that didn't fail with internal or customer error.
        # This is required to simplify the protocol so Server doesn't need to care about task states
        # and cancel each tasks carefully to not get its outcome as failed.
        with (
            metric_tasks_uploading_outputs.track_inprogress(),
            metric_task_output_upload_latency.time(),
        ):
            metric_task_output_uploads.inc()
            await self._upload_task_output(output)

        _report_task_completion_metrics(
            task_outcome_code=output.outcome_code,
            task_failure_reason=output.failure_reason,
            logger=self._logger,
        )

        self._state_reporter.add_completed_task_output(output)
        # Report the outcome to the Server asap to reduce latency.
        self._state_reporter.schedule_state_report()

    async def _download_inputs(self) -> None:
        """Downloads the task inputs and init value.

        Raises an Exception if the inputs failed to download.
        """
        self._input = await self._downloader.download_input(
            data_payload=self._task.input,
            logger=self._logger,
        )

        if self._task.HasField("reducer_input"):
            self._init_value = await self._downloader.download_init_value(
                data_payload=self._task.reducer_input,
                logger=self._logger,
            )

    async def _run_task_when_function_executor_is_available(self) -> TaskOutput:
        """Runs the task on the Function Executor when it's available.

        Raises an Exception if task failed due to an internal error."""
        fe_terminated_task_output: Optional[TaskOutput] = (
            await self._acquire_function_executor()
        )
        if fe_terminated_task_output is not None:
            return fe_terminated_task_output

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
            else:
                # TODO: only run the health check if the task failed.
                # If the task failed due to FE being unhealthy then report it to server asap to not schedule more tasks on it.
                # This reduces latency in the system.
                result: HealthCheckResult = (
                    await self._function_executor_state.function_executor.health_checker().check()
                )
                if not result.is_healthy:
                    next_status = FunctionExecutorStatus.UNHEALTHY
                    self._logger.error(
                        "Function Executor health check failed, marking Function Executor as unhealthy",
                        health_check_fail_reason=result.reason,
                    )

            await self._release_function_executor(next_status=next_status)

    async def _acquire_function_executor(self) -> Optional[TaskOutput]:
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
                    allowlist=[
                        FunctionExecutorStatus.IDLE,
                        FunctionExecutorStatus.SHUTDOWN,
                    ],
                )
                if self._function_executor_state.status == FunctionExecutorStatus.IDLE:
                    await self._function_executor_state.set_status(
                        FunctionExecutorStatus.RUNNING_TASK
                    )
                    # At this point the Function Executor belongs to this task controller due to RUNNING_TASK status.
                    # We can now unlock the FE state. We have to update the FE status once the task succeeds or fails.
                else:
                    # Dirty handling of FE failure.
                    return self._function_executor_terminated_output()

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

    async def _upload_task_output(self, output: TaskOutput) -> None:
        """Uploads the task output to blob store.

        Doesn't raise any Exceptions. Runs till the reporting is successful."""
        upload_retries: int = 0

        while True:
            logger = self._logger.bind(retries=upload_retries)
            try:
                await self._task_output_uploader.upload(output=output, logger=logger)
                break
            except Exception as e:
                logger.error(
                    "failed to upload task output",
                    exc_info=e,
                )
                upload_retries += 1
                metric_task_output_upload_retries.inc()
                await asyncio.sleep(_TASK_OUTPUT_UPLOAD_BACKOFF_SEC)

    def _internal_error_output(self) -> TaskOutput:
        return TaskOutput.internal_error(
            task_id=self._task.id,
            namespace=self._task.namespace,
            graph_name=self._task.graph_name,
            function_name=self._task.function_name,
            graph_version=self._task.graph_version,
            graph_invocation_id=self._task.graph_invocation_id,
            output_payload_uri_prefix=self._task.output_payload_uri_prefix,
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
            output_payload_uri_prefix=self._task.output_payload_uri_prefix,
        )

    def _task_cancelled_output(self) -> TaskOutput:
        return TaskOutput.task_cancelled(
            task_id=self._task.id,
            namespace=self._task.namespace,
            graph_name=self._task.graph_name,
            function_name=self._task.function_name,
            graph_version=self._task.graph_version,
            graph_invocation_id=self._task.graph_invocation_id,
            output_payload_uri_prefix=self._task.output_payload_uri_prefix,
        )

    def _function_executor_terminated_output(self) -> TaskOutput:
        return TaskOutput.function_executor_terminated(
            task_id=self._task.id,
            namespace=self._task.namespace,
            graph_name=self._task.graph_name,
            function_name=self._task.function_name,
            graph_version=self._task.graph_version,
            graph_invocation_id=self._task.graph_invocation_id,
            output_payload_uri_prefix=self._task.output_payload_uri_prefix,
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
        output_payload_uri_prefix=task.output_payload_uri_prefix,
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


def _report_task_completion_metrics(
    task_outcome_code: TaskOutcomeCode,
    task_failure_reason: TaskFailureReason,  # None on success
    logger: Any,
) -> None:
    metric_tasks_completed.labels(
        outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_ALL,
        failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_ALL,
    ).inc()
    if task_outcome_code == TaskOutcomeCode.TASK_OUTCOME_CODE_SUCCESS:
        metric_tasks_completed.labels(
            outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_SUCCESS,
            failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_NONE,
        ).inc()
    elif task_outcome_code == TaskOutcomeCode.TASK_OUTCOME_CODE_FAILURE:
        if task_failure_reason == TaskFailureReason.TASK_FAILURE_REASON_INTERNAL_ERROR:
            metric_tasks_completed.labels(
                outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_INTERNAL_ERROR,
            ).inc()
        elif (
            task_failure_reason
            == TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED
        ):
            metric_tasks_completed.labels(
                outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_FUNCTION_EXECUTOR_TERMINATED,
            ).inc()
        elif task_failure_reason in [
            TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_ERROR,
            TaskFailureReason.TASK_FAILURE_REASON_FUNCTION_TIMEOUT,
        ]:
            metric_tasks_completed.labels(
                outcome_code=METRIC_TASKS_COMPLETED_OUTCOME_CODE_FAILURE,
                failure_reason=METRIC_TASKS_COMPLETED_FAILURE_REASON_FUNCTION_ERROR,
            ).inc()
        else:
            logger.warning(
                "unknown task failure reason, not reporting task_completed metric",
                failure_reason=TaskFailureReason.Name(task_failure_reason),
            )


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
