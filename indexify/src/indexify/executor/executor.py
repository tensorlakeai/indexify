import asyncio
import signal
import time
from pathlib import Path
from socket import gethostname
from typing import Any, Dict, List, Optional

import structlog
from tensorlake.function_executor.proto.function_executor_pb2 import SerializedObject
from tensorlake.utils.logging import suppress as suppress_logging

from indexify.proto.executor_api_pb2 import ExecutorStatus

from .api_objects import FunctionURI, Task
from .downloader import Downloader
from .executor_flavor import ExecutorFlavor
from .function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from .function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerFactory,
)
from .grpc.channel_manager import ChannelManager
from .grpc.state_reconciler import ExecutorStateReconciler
from .grpc.state_reporter import ExecutorStateReporter
from .metrics.executor import (
    METRIC_TASKS_COMPLETED_OUTCOME_ALL,
    METRIC_TASKS_COMPLETED_OUTCOME_ERROR_CUSTOMER_CODE,
    METRIC_TASKS_COMPLETED_OUTCOME_ERROR_PLATFORM,
    METRIC_TASKS_COMPLETED_OUTCOME_SUCCESS,
    metric_executor_info,
    metric_executor_state,
    metric_task_completion_latency,
    metric_task_outcome_report_latency,
    metric_task_outcome_report_retries,
    metric_task_outcome_reports,
    metric_tasks_completed,
    metric_tasks_fetched,
    metric_tasks_reporting_outcome,
)
from .monitoring.function_allowlist import function_allowlist_to_info_dict
from .monitoring.health_check_handler import HealthCheckHandler
from .monitoring.health_checker.health_checker import HealthChecker
from .monitoring.prometheus_metrics_handler import PrometheusMetricsHandler
from .monitoring.server import MonitoringServer
from .monitoring.startup_probe_handler import StartupProbeHandler
from .task_fetcher import TaskFetcher
from .task_reporter import TaskReporter
from .task_runner import TaskInput, TaskOutput, TaskRunner

metric_executor_state.state("starting")


class Executor:
    def __init__(
        self,
        id: str,
        development_mode: bool,
        flavor: ExecutorFlavor,
        version: str,
        labels: Dict[str, str],
        code_path: Path,
        health_checker: HealthChecker,
        function_allowlist: Optional[List[FunctionURI]],
        function_executor_server_factory: FunctionExecutorServerFactory,
        server_addr: str,
        config_path: Optional[str],
        monitoring_server_host: str,
        monitoring_server_port: int,
        grpc_server_addr: Optional[str],
        enable_grpc_state_reconciler: bool,
    ):
        self._logger = structlog.get_logger(module=__name__)
        self._is_shutdown: bool = False
        protocol: str = "http"
        if config_path:
            self._logger.info("running the extractor with TLS enabled")
            protocol = "https"

        self._server_addr = server_addr
        self._base_url = f"{protocol}://{self._server_addr}"
        self._code_path = code_path
        self._startup_probe_handler = StartupProbeHandler()
        self._monitoring_server = MonitoringServer(
            host=monitoring_server_host,
            port=monitoring_server_port,
            startup_probe_handler=self._startup_probe_handler,
            health_probe_handler=HealthCheckHandler(health_checker),
            metrics_handler=PrometheusMetricsHandler(),
        )
        self._function_executor_states = FunctionExecutorStatesContainer(
            logger=self._logger
        )
        health_checker.set_function_executor_states_container(
            self._function_executor_states
        )
        self._downloader = Downloader(
            code_path=code_path, base_url=self._base_url, config_path=config_path
        )
        self._task_reporter = TaskReporter(
            base_url=self._base_url,
            executor_id=id,
            config_path=config_path,
        )
        self._function_allowlist: Optional[List[FunctionURI]] = function_allowlist
        self._function_executor_server_factory = function_executor_server_factory

        # HTTP mode services
        self._task_runner: Optional[TaskRunner] = None
        self._task_fetcher: Optional[TaskFetcher] = None
        # gRPC mode services
        self._channel_manager: Optional[ChannelManager] = None
        self._state_reporter: Optional[ExecutorStateReporter] = None
        self._state_reconciler: Optional[ExecutorStateReconciler] = None

        if grpc_server_addr is not None:
            self._channel_manager = ChannelManager(
                server_address=grpc_server_addr,
                config_path=config_path,
                logger=self._logger,
            )
            self._state_reporter = ExecutorStateReporter(
                executor_id=id,
                flavor=flavor,
                version=version,
                labels=labels,
                development_mode=development_mode,
                function_allowlist=self._function_allowlist,
                function_executor_states=self._function_executor_states,
                channel_manager=self._channel_manager,
                logger=self._logger,
            )
            self._state_reporter.update_executor_status(
                ExecutorStatus.EXECUTOR_STATUS_STARTING_UP
            )

        if enable_grpc_state_reconciler:
            self._state_reconciler = ExecutorStateReconciler(
                executor_id=id,
                function_executor_server_factory=self._function_executor_server_factory,
                base_url=self._base_url,
                function_executor_states=self._function_executor_states,
                config_path=config_path,
                downloader=self._downloader,
                task_reporter=self._task_reporter,
                channel_manager=self._channel_manager,
                state_reporter=self._state_reporter,
                logger=self._logger,
            )
        else:
            self._task_runner = TaskRunner(
                executor_id=id,
                function_executor_server_factory=function_executor_server_factory,
                base_url=self._base_url,
                function_executor_states=self._function_executor_states,
                config_path=config_path,
            )
            self._task_fetcher = TaskFetcher(
                executor_id=id,
                executor_version=version,
                labels=labels,
                function_allowlist=function_allowlist,
                protocol=protocol,
                indexify_server_addr=self._server_addr,
                config_path=config_path,
            )

        executor_info: Dict[str, str] = {
            "id": id,
            "dev_mode": str(development_mode),
            "version": version,
            "code_path": str(code_path),
            "server_addr": server_addr,
            "config_path": str(config_path),
            "grpc_server_addr": str(grpc_server_addr),
            "enable_grpc_state_reconciler": str(enable_grpc_state_reconciler),
            "hostname": gethostname(),
        }
        executor_info.update(function_allowlist_to_info_dict(function_allowlist))
        metric_executor_info.info(executor_info)

    def run(self):
        asyncio.new_event_loop()
        for signum in [
            signal.SIGABRT,
            signal.SIGINT,
            signal.SIGTERM,
            signal.SIGQUIT,
            signal.SIGHUP,
        ]:
            asyncio.get_event_loop().add_signal_handler(
                signum, self.shutdown, asyncio.get_event_loop()
            )

        asyncio.get_event_loop().create_task(self._monitoring_server.run())
        if self._state_reporter is not None:
            self._state_reporter.update_executor_status(
                ExecutorStatus.EXECUTOR_STATUS_RUNNING
            )
            asyncio.get_event_loop().create_task(self._state_reporter.run())

        metric_executor_state.state("running")
        self._startup_probe_handler.set_ready()

        try:
            if self._state_reconciler is None:
                asyncio.get_event_loop().run_until_complete(
                    self._http_task_runner_loop()
                )
            else:
                asyncio.get_event_loop().run_until_complete(
                    self._grpc_state_reconciler_loop()
                )
        except asyncio.CancelledError:
            pass  # Suppress this expected exception and return without error (normally).

    async def _grpc_state_reconciler_loop(self):
        """Runs the gRPC state reconciler and state reporter.

        Never raises any exceptions."""
        asyncio.create_task(self._state_reporter.run())
        await self._state_reconciler.run()

    async def _http_task_runner_loop(self):
        while not self._is_shutdown:
            try:
                async for task in self._task_fetcher.run():
                    metric_tasks_fetched.inc()
                    if not self._is_shutdown:
                        asyncio.create_task(self._run_task(task))
            except Exception as e:
                self._logger.error(
                    "failed fetching tasks, retrying in 5 seconds", exc_info=e
                )
                await asyncio.sleep(5)

    async def _run_task(self, task: Task) -> None:
        """Runs the supplied task.

        Doesn't raise any Exceptions. All errors are reported to the server."""
        start_time: float = time.monotonic()
        logger = self._task_logger(task)
        output: Optional[TaskOutput] = None

        try:
            output = await self._run_task_and_get_output(task, logger)
            logger.info("task execution finished", success=output.success)
        except Exception as e:
            output = TaskOutput.internal_error(
                task_id=task.id,
                namespace=task.namespace,
                graph_name=task.compute_graph,
                function_name=task.compute_fn,
                graph_version=task.graph_version,
                graph_invocation_id=task.invocation_id,
            )
            logger.error("task execution failed", exc_info=e)

        with (
            metric_tasks_reporting_outcome.track_inprogress(),
            metric_task_outcome_report_latency.time(),
        ):
            metric_task_outcome_reports.inc()
            await self._report_task_outcome(output=output, logger=logger)

        metric_task_completion_latency.observe(time.monotonic() - start_time)

    async def _run_task_and_get_output(self, task: Task, logger: Any) -> TaskOutput:
        graph: SerializedObject = await self._downloader.download_graph(
            namespace=task.namespace,
            graph_name=task.compute_graph,
            graph_version=task.graph_version,
            logger=logger,
        )
        input: SerializedObject = await self._downloader.download_input(
            namespace=task.namespace,
            graph_name=task.compute_graph,
            graph_invocation_id=task.invocation_id,
            input_key=task.input_key,
            logger=logger,
        )
        init_value: Optional[SerializedObject] = (
            None
            if task.reducer_output_id is None
            else (
                await self._downloader.download_init_value(
                    namespace=task.namespace,
                    graph_name=task.compute_graph,
                    function_name=task.compute_fn,
                    graph_invocation_id=task.invocation_id,
                    reducer_output_key=task.reducer_output_id,
                    logger=logger,
                )
            )
        )
        return await self._task_runner.run(
            TaskInput(
                task=task,
                graph=graph,
                input=input,
                init_value=init_value,
            ),
            logger=logger,
        )

    async def _report_task_outcome(self, output: TaskOutput, logger: Any) -> None:
        """Reports the task with the given output to the server.

        Doesn't raise any Exceptions. Runs till the reporting is successful."""
        reporting_retries: int = 0

        while True:
            logger = logger.bind(retries=reporting_retries)
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
                await asyncio.sleep(5)

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

    async def _shutdown(self, loop):
        self._logger.info(
            "shutting down, all Executor logs are suppressed, no task outcomes will be reported to Server from this point"
        )
        if self._state_reporter is not None:
            self._state_reporter.update_executor_status(
                ExecutorStatus.EXECUTOR_STATUS_STOPPING
            )
        metric_executor_state.state("shutting_down")
        # There will be lots of task cancellation exceptions and "X is shutting down"
        # exceptions logged during Executor shutdown. Suppress their logs as they are
        # expected and are confusing for users.
        suppress_logging()

        self._is_shutdown = True
        await self._monitoring_server.shutdown()
        await self._task_reporter.shutdown()

        if self._task_runner is not None:
            await self._task_runner.shutdown()

        if self._channel_manager is not None:
            await self._channel_manager.shutdown()
        if self._state_reporter is not None:
            await self._state_reporter.shutdown()
        if self._state_reconciler is not None:
            await self._state_reconciler.shutdown()

        # We need to shutdown all users of FE states first,
        # otherwise states might disappear unexpectedly and we might
        # report errors, etc that are expected.
        await self._function_executor_states.shutdown()
        # We mainly need to cancel the task that runs _.*_mode_loop().
        for task in asyncio.all_tasks(loop):
            task.cancel()
        # The current task is cancelled, the code after this line will not run.

    def shutdown(self, loop):
        loop.create_task(self._shutdown(loop))

    def _task_logger(self, task: Task) -> Any:
        return self._logger.bind(
            namespace=task.namespace,
            graph=task.compute_graph,
            graph_version=task.graph_version,
            invocation_id=task.invocation_id,
            function_name=task.compute_fn,
            task_id=task.id,
        )
