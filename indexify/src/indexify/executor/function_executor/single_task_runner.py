from collections.abc import Awaitable, Callable
from typing import Any, Optional

import grpc
from tensorlake.function_executor.proto.function_executor_pb2 import (
    InitializeRequest,
    RunTaskRequest,
    RunTaskResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

from ..api_objects import Task
from .function_executor import CustomerError, FunctionExecutor
from .function_executor_state import FunctionExecutorState
from .function_executor_status import FunctionExecutorStatus
from .health_checker import HealthChecker, HealthCheckResult
from .metrics.single_task_runner import (
    metric_function_executor_run_task_rpc_errors,
    metric_function_executor_run_task_rpc_latency,
    metric_function_executor_run_task_rpcs,
)
from .server.function_executor_server_factory import (
    FunctionExecutorServerConfiguration,
    FunctionExecutorServerFactory,
)
from .task_input import TaskInput
from .task_output import TaskOutput


class SingleTaskRunner:
    def __init__(
        self,
        executor_id: str,
        function_executor_state: FunctionExecutorState,
        task_input: TaskInput,
        function_executor_server_factory: FunctionExecutorServerFactory,
        base_url: str,
        config_path: Optional[str],
        logger: Any,
    ):
        self._executor_id: str = executor_id
        self._function_executor_state: FunctionExecutorState = function_executor_state
        self._task_input: TaskInput = task_input
        self._function_executor_server_factory: FunctionExecutorServerFactory = (
            function_executor_server_factory
        )
        self._base_url: str = base_url
        self._config_path: Optional[str] = config_path
        self._logger = logger.bind(module=__name__)

    async def run(self) -> TaskOutput:
        """Runs the task in the Function Executor.

        The FunctionExecutorState must be locked by the caller.
        The lock is released during actual task run in the server.
        The lock is relocked on return.

        Raises an exception if an error occured.

        On enter the Function Executor status is either IDLE, UNHEALTHY or DESTROYED.
        On return the Function Executor status is either IDLE, UNHEALTHY or DESTROYED.
        """
        self._function_executor_state.check_locked()

        if self._function_executor_state.status not in [
            FunctionExecutorStatus.IDLE,
            FunctionExecutorStatus.UNHEALTHY,
            FunctionExecutorStatus.DESTROYED,
        ]:
            self._logger.error(
                "Function Executor is not in oneof [IDLE, UNHEALTHY, DESTROYED] state, cannot run the task",
                status=self._function_executor_state.status,
            )
            raise RuntimeError(
                f"Unexpected Function Executor state {self._function_executor_state.status}"
            )

        # If Function Executor became unhealthy while was idle then destroy it.
        # It'll be recreated below.
        await self._destroy_existing_function_executor_if_unhealthy()

        # Create Function Executor if it doesn't exist yet.
        if self._function_executor_state.status == FunctionExecutorStatus.DESTROYED:
            try:
                await self._create_function_executor()
            except CustomerError as e:
                return TaskOutput(
                    task_id=self._task_input.task.id,
                    namespace=self._task_input.task.namespace,
                    graph_name=self._task_input.task.compute_graph,
                    function_name=self._task_input.task.compute_fn,
                    graph_version=self._task_input.task.graph_version,
                    graph_invocation_id=self._task_input.task.invocation_id,
                    stderr=str(e),
                    success=False,
                )

        try:
            return await self._run()
        finally:
            # If Function Executor became unhealthy while running the task then destroy it.
            # The periodic health checker might not notice this as it does only periodic checks.
            await self._destroy_existing_function_executor_if_unhealthy()

            if self._function_executor_state.status not in [
                FunctionExecutorStatus.IDLE,
                FunctionExecutorStatus.UNHEALTHY,
                FunctionExecutorStatus.DESTROYED,
            ]:
                self._logger.error(
                    "Function Executor status is not oneof [IDLE, UNHEALTHY, DESTROYED] after running the task, resetting the state to mitigate a possible bug",
                    status=self._function_executor_state.status,
                )
                if self._function_executor_state.function_executor is None:
                    await self._function_executor_state.set_status(
                        FunctionExecutorStatus.DESTROYED
                    )
                else:
                    await self._function_executor_state.set_status(
                        FunctionExecutorStatus.UNHEALTHY
                    )

    async def _create_function_executor(self) -> None:
        await self._function_executor_state.set_status(
            FunctionExecutorStatus.STARTING_UP
        )
        self._function_executor_state.function_executor = FunctionExecutor(
            server_factory=self._function_executor_server_factory, logger=self._logger
        )
        config: FunctionExecutorServerConfiguration = (
            FunctionExecutorServerConfiguration(
                executor_id=self._executor_id,
                function_executor_id=self._function_executor_state.id,
                namespace=self._task_input.task.namespace,
                image_uri=self._task_input.task.image_uri,
                secret_names=self._task_input.task.secret_names or [],
            )
        )
        initialize_request: InitializeRequest = InitializeRequest(
            namespace=self._task_input.task.namespace,
            graph_name=self._task_input.task.compute_graph,
            graph_version=self._task_input.task.graph_version,
            function_name=self._task_input.task.compute_fn,
            graph=self._task_input.graph,
        )

        try:
            await self._function_executor_state.function_executor.initialize(
                config=config,
                initialize_request=initialize_request,
                base_url=self._base_url,
                config_path=self._config_path,
            )
        except CustomerError:
            # We have to follow the valid state transition sequence.
            await self._function_executor_state.set_status(
                FunctionExecutorStatus.STARTUP_FAILED_CUSTOMER_ERROR
            )
            await self._function_executor_state.destroy_function_executor()
            raise
        except Exception:
            # We have to follow the valid state transition sequence.
            await self._function_executor_state.set_status(
                FunctionExecutorStatus.STARTUP_FAILED_PLATFORM_ERROR
            )
            await self._function_executor_state.destroy_function_executor()
            raise

        await self._function_executor_state.set_status(FunctionExecutorStatus.IDLE)

    async def _run(self) -> TaskOutput:
        request: RunTaskRequest = RunTaskRequest(
            namespace=self._task_input.task.namespace,
            graph_name=self._task_input.task.compute_graph,
            graph_version=self._task_input.task.graph_version,
            function_name=self._task_input.task.compute_fn,
            graph_invocation_id=self._task_input.task.invocation_id,
            task_id=self._task_input.task.id,
            function_input=self._task_input.input,
        )
        if self._task_input.init_value is not None:
            request.function_init_value.CopyFrom(self._task_input.init_value)
        channel: grpc.aio.Channel = (
            self._function_executor_state.function_executor.channel()
        )

        async with _RunningTaskContextManager(
            invocation_id=self._task_input.task.invocation_id,
            task_id=self._task_input.task.id,
            health_check_failed_callback=self._health_check_failed_callback,
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
                response: RunTaskResponse = await FunctionExecutorStub(
                    channel
                ).run_task(request)
            return _task_output(task=self._task_input.task, response=response)

    async def _health_check_failed_callback(self, result: HealthCheckResult):
        # Function Executor destroy due to the periodic health check failure ensures that
        # a running task RPC stuck in unhealthy Function Executor fails immidiately.
        async with self._function_executor_state.lock:
            if (
                self._function_executor_state.status
                != FunctionExecutorStatus.RUNNING_TASK
            ):
                # Protection in case the callback gets delivered after we finished running the task.
                return

            await self._function_executor_state.set_status(
                FunctionExecutorStatus.UNHEALTHY
            )
            await self._destroy_function_executor_on_failed_health_check(result.reason)

    async def _destroy_existing_function_executor_if_unhealthy(self):
        self._function_executor_state.check_locked()
        if self._function_executor_state.status == FunctionExecutorStatus.IDLE:
            result: HealthCheckResult = (
                await self._function_executor_state.function_executor.health_checker().check()
            )
            if not result.is_healthy:
                await self._function_executor_state.set_status(
                    FunctionExecutorStatus.UNHEALTHY
                )

        if self._function_executor_state.status == FunctionExecutorStatus.UNHEALTHY:
            await self._destroy_function_executor_on_failed_health_check(result.reason)

    async def _destroy_function_executor_on_failed_health_check(self, reason: str):
        self._function_executor_state.check_locked()
        self._logger.error(
            "Function Executor health check failed, destroying Function Executor",
            health_check_fail_reason=reason,
        )
        await self._function_executor_state.destroy_function_executor()


class _RunningTaskContextManager:
    """Performs all the actions required before and after running a task."""

    def __init__(
        self,
        invocation_id: str,
        task_id: str,
        health_check_failed_callback: Callable[[], Awaitable[None]],
        function_executor_state: FunctionExecutorState,
    ):
        self._invocation_id: str = invocation_id
        self._task_id: str = task_id
        self._health_check_failed_callback: Callable[[], Awaitable[None]] = (
            health_check_failed_callback
        )
        self._state: FunctionExecutorState = function_executor_state

    async def __aenter__(self):
        await self._state.set_status(FunctionExecutorStatus.RUNNING_TASK)
        self._state.function_executor.invocation_state_client().add_task_to_invocation_id_entry(
            task_id=self._task_id,
            invocation_id=self._invocation_id,
        )
        self._state.function_executor.health_checker().start(
            self._health_check_failed_callback
        )
        # Unlock the state so other tasks can act depending on it.
        self._state.lock.release()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._state.lock.acquire()
        # Health check callback could destroy the FunctionExecutor and set status to UNHEALTHY
        if self._state.status == FunctionExecutorStatus.RUNNING_TASK:
            await self._state.set_status(FunctionExecutorStatus.IDLE)
            self._state.function_executor.invocation_state_client().remove_task_to_invocation_id_entry(
                task_id=self._task_id
            )
            self._state.function_executor.health_checker().stop()


def _task_output(task: Task, response: RunTaskResponse) -> TaskOutput:
    required_fields = [
        "stdout",
        "stderr",
        "is_reducer",
        "success",
    ]

    for field in required_fields:
        if not response.HasField(field):
            raise ValueError(f"Response is missing required field: {field}")

    output = TaskOutput(
        task_id=task.id,
        namespace=task.namespace,
        graph_name=task.compute_graph,
        function_name=task.compute_fn,
        graph_version=task.graph_version,
        graph_invocation_id=task.invocation_id,
        stdout=response.stdout,
        stderr=response.stderr,
        reducer=response.is_reducer,
        success=response.success,
    )

    if response.HasField("function_output"):
        output.function_output = response.function_output
    if response.HasField("router_output"):
        output.router_output = response.router_output

    return output
