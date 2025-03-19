import asyncio
from typing import Any, AsyncGenerator, List, Optional, Set

import grpc
from tensorlake.function_executor.proto.function_executor_pb2 import (
    InitializeRequest,
    SerializedObject,
)

from indexify.proto.executor_api_pb2 import (
    DesiredExecutorState,
    FunctionExecutorDescription,
    FunctionExecutorStatus,
    GetDesiredExecutorStatesRequest,
)
from indexify.proto.executor_api_pb2_grpc import (
    ExecutorAPIStub,
)

from ..downloader import Downloader
from ..function_executor.function_executor import CustomerError, FunctionExecutor
from ..function_executor.function_executor_state import FunctionExecutorState
from ..function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from ..function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerConfiguration,
    FunctionExecutorServerFactory,
)
from ..function_executor.task_input import TaskInput
from ..function_executor.task_output import TaskOutput
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
from ..task_reporter import TaskReporter
from .channel_manager import ChannelManager
from .state_reporter import ExecutorStateReporter

_RECONCILE_STREAM_BACKOFF_INTERVAL_SEC = 5


class ExecutorStateReconciler:
    def __init__(
        self,
        executor_id: str,
        function_executor_server_factory: FunctionExecutorServerFactory,
        base_url: str,
        function_executor_states: FunctionExecutorStatesContainer,
        config_path: Optional[str],
        downloader: Downloader,
        task_reporter: TaskReporter,
        channel_manager: ChannelManager,
        state_reporter: ExecutorStateReporter,
        logger: Any,
    ):
        self._executor_id: str = executor_id
        self._function_executor_server_factory: FunctionExecutorServerFactory = (
            function_executor_server_factory
        )
        self._base_url: str = base_url
        self._config_path: Optional[str] = config_path
        self._downloader: Downloader = downloader
        self._task_reporter: TaskReporter = task_reporter
        self._function_executor_states: FunctionExecutorStatesContainer = (
            function_executor_states
        )
        self._channel_manager: ChannelManager = channel_manager
        self._state_reporter: ExecutorStateReporter = state_reporter
        self._logger: Any = logger.bind(module=__name__)
        self._is_shutdown: bool = False
        self._server_last_clock: Optional[int] = None

    async def run(self):
        """Runs the state reconciler.

        Never raises any exceptions.
        """
        while not self._is_shutdown:
            async with await self._channel_manager.get_channel() as server_channel:
                server_channel: grpc.aio.Channel
                stub = ExecutorAPIStub(server_channel)
                while not self._is_shutdown:
                    try:
                        # Report state once before starting the stream so Server
                        # doesn't use old state it knew about this Executor in the past.
                        await self._state_reporter.report_state(stub)
                        desired_states_stream: AsyncGenerator[
                            DesiredExecutorState, None
                        ] = stub.get_desired_executor_states(
                            GetDesiredExecutorStatesRequest(
                                executor_id=self._executor_id
                            )
                        )
                        await self._process_desired_states_stream(desired_states_stream)
                    except Exception as e:
                        self._logger.error(
                            f"Failed processing desired states stream, reconnecting in {_RECONCILE_STREAM_BACKOFF_INTERVAL_SEC} sec.",
                            exc_info=e,
                        )
                        await asyncio.sleep(_RECONCILE_STREAM_BACKOFF_INTERVAL_SEC)
                        break

        self._logger.info("State reconciler shutdown.")

    async def _process_desired_states_stream(
        self, desired_states: AsyncGenerator[DesiredExecutorState, None]
    ):
        async for new_state in desired_states:
            if self._is_shutdown:
                return

            new_state: DesiredExecutorState
            if self._server_last_clock is not None:
                if self._server_last_clock >= new_state.clock:
                    continue  # Duplicate or outdated message state sent by Server.

            self._server_last_clock = new_state.clock
            await self._reconcile_state(new_state)

    async def _reconcile_state(self, new_state: DesiredExecutorState):
        await self._reconcile_function_executors(new_state)
        # TODO
        # await self._reconcile_task_allocations(new_state)

    async def shutdown(self):
        """Shuts down the state reconciler.

        Never raises any exceptions.
        """
        self._is_shutdown = True

    async def _reconcile_function_executors(self, desired_state: DesiredExecutorState):
        desired_function_executor_ids: Set[str] = set()
        for desired_function_executor in desired_state.function_executors:
            desired_function_executor: FunctionExecutorDescription
            desired_function_executor_ids.add(desired_function_executor.id)

            function_executor_state: FunctionExecutorState = (
                self._function_executor_states.get_or_create_state(
                    id=desired_function_executor.id,
                    namespace=desired_function_executor.namespace,
                    graph_name=desired_function_executor.graph_name,
                    graph_version=desired_function_executor.graph_version,
                    function_name=desired_function_executor.function_name,
                    image_uri=desired_function_executor.image_uri,
                )
            )

            async with function_executor_state.lock:
                if (
                    function_executor_state.status
                    == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_STOPPED
                ):
                    function_executor_state.status = (
                        FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_STARTING_UP
                    )
                    try:
                        function_executor_state.function_executor = (
                            await self._create_function_executor()
                        )
                        function_executor_state.status = (
                            FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_IDLE
                        )
                    except CustomerError as e:
                        function_executor_state.status = (
                            FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_CUSTOMER_ERROR
                        )
                    except Exception as e:
                        function_executor_state.status = (
                            FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_PLATFORM_ERROR
                        )
                        self._logger.error(
                            f"Failed to create Function Executor", exc_info=e
                        )

        function_executor_state_ids_to_destroy: List[str] = []
        async for function_executor_state in self._function_executor_states:
            function_executor_state: FunctionExecutorState
            if function_executor_state.id not in desired_function_executor_ids:
                function_executor_state_ids_to_destroy.append(
                    function_executor_state.id
                )

        for function_executor_state_id in function_executor_state_ids_to_destroy:
            function_executor_state: FunctionExecutorState = (
                self._function_executor_states.pop_state(function_executor_state_id)
            )
            async with function_executor_state.lock:
                logger = self._function_executor_logger(
                    id=function_executor_state.id,
                    namespace=function_executor_state.namespace,
                    graph_name=function_executor_state.graph_name,
                    graph_version=function_executor_state.graph_version,
                    function_name=function_executor_state.function_name,
                )
                if (
                    function_executor_state.status
                    == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_RUNNING_TASK
                ):
                    logger.warning(
                        "Destroying Function Executor that is running a task. No task output will be reported as this is expected by the Server."
                    )
                function_executor_state.status = (
                    FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_STOPPING
                )
                await function_executor_state.destroy_function_executor()
                function_executor_state.status = (
                    FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_STOPPED
                )

    async def _create_function_executor(
        self, description: FunctionExecutorDescription
    ) -> FunctionExecutor:
        logger = self._function_executor_logger(
            id=description.id,
            namespace=description.namespace,
            graph_name=description.graph_name,
            graph_version=description.graph_version,
            function_name=description.function_name,
        )
        graph: SerializedObject = await self._downloader.download_graph(
            namespace=description.namespace,
            graph_name=description.graph_name,
            graph_version=description.graph_version,
            logger=logger,
        )
        function_executor: FunctionExecutor = FunctionExecutor(
            server_factory=self._function_executor_server_factory, logger=logger
        )
        config: FunctionExecutorServerConfiguration = (
            FunctionExecutorServerConfiguration(
                executor_id=self._executor_id,
                function_executor_id=description.id,
                namespace=description.namespace,
                image_uri=description.image_uri,
                secret_names=list(description.secret_names),
            )
        )
        initialize_request: InitializeRequest = InitializeRequest(
            namespace=description.namespace,
            graph_name=description.graph_name,
            graph_version=description.graph_version,
            function_name=description.function_name,
            graph=graph,
        )

        try:
            await function_executor.initialize(
                config=config,
                initialize_request=initialize_request,
                base_url=self._base_url,
                config_path=self._config_path,
            )
            return function_executor
        except Exception:
            await function_executor.destroy()
            raise

    async def _cancel_running_tasks(
        self, function_executor_state: FunctionExecutorState
    ):
        pass

    def _function_executor_logger(
        self,
        id: str,
        namespace: str,
        graph_name: str,
        graph_version: str,
        function_name: str,
    ) -> Any:
        return self._logger.bind(
            id=id,
            namespace=namespace,
            graph=graph_name,
            graph_version=graph_version,
            function_name=function_name,
        )

    async def _report_task_outcome(self, task_output: TaskOutput):
        """Reports the task with the given output to the server.

        Doesn't raise any Exceptions. Runs till the reporting is successful."""
        reporting_retries: int = 0

        while True:
            logger = logger.bind(retries=reporting_retries)
            try:
                await self._task_reporter.report(output=task_output, logger=logger)
                break
            except Exception as e:
                logger.error(
                    "failed to report task",
                    exc_info=e,
                )
                reporting_retries += 1
                metric_task_outcome_report_retries.inc()
                await asyncio.sleep(5)

        metric_tasks_completed.labels(outcome=METRIC_TASKS_COMPLETED_OUTCOME_ALL).inc()
        if task_output.is_internal_error:
            metric_tasks_completed.labels(
                outcome=METRIC_TASKS_COMPLETED_OUTCOME_ERROR_PLATFORM
            ).inc()
        elif task_output.success:
            metric_tasks_completed.labels(
                outcome=METRIC_TASKS_COMPLETED_OUTCOME_SUCCESS
            ).inc()
        else:
            metric_tasks_completed.labels(
                outcome=METRIC_TASKS_COMPLETED_OUTCOME_ERROR_CUSTOMER_CODE
            ).inc()
