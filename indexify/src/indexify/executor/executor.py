import asyncio
import signal
import time
from pathlib import Path
from socket import gethostname
from typing import Any, Dict, List, Optional

import structlog
from tensorlake.function_executor.proto.function_executor_pb2 import SerializedObject
from tensorlake.utils.logging import suppress as suppress_logging

from .api_objects import FunctionURI, Task
from .downloader import Downloader
from .function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from .function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerFactory,
)
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
        version: str,
        code_path: Path,
        health_checker: HealthChecker,
        function_allowlist: Optional[List[FunctionURI]],
        function_executor_server_factory: FunctionExecutorServerFactory,
        server_addr: str,
        config_path: Optional[str],
        monitoring_server_host: str,
        monitoring_server_port: int,
        disable_automatic_function_executor_management: bool,
    ):
        self._logger = structlog.get_logger(module=__name__)
        self._is_shutdown: bool = False
        self._config_path = config_path
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
        self._function_executor_states = FunctionExecutorStatesContainer()
        health_checker.set_function_executor_states_container(
            self._function_executor_states
        )
        self._task_runner = TaskRunner(
            executor_id=id,
            function_executor_server_factory=function_executor_server_factory,
            base_url=self._base_url,
            disable_automatic_function_executor_management=disable_automatic_function_executor_management,
            function_executor_states=self._function_executor_states,
            config_path=config_path,
        )
        self._downloader = Downloader(
            code_path=code_path, base_url=self._base_url, config_path=config_path
        )
        self._task_fetcher = TaskFetcher(
            executor_id=id,
            executor_version=version,
            function_allowlist=function_allowlist,
            protocol=protocol,
            indexify_server_addr=self._server_addr,
            config_path=config_path,
        )
        self._task_reporter = TaskReporter(
            base_url=self._base_url,
            executor_id=id,
            config_path=self._config_path,
        )
        executor_info: Dict[str, str] = {
            "id": id,
            "version": version,
            "code_path": str(code_path),
            "server_addr": server_addr,
            "config_path": str(config_path),
            "disable_automatic_function_executor_management": str(
                disable_automatic_function_executor_management
            ),
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

        try:
            asyncio.get_event_loop().run_until_complete(self._run_tasks_loop())
        except asyncio.CancelledError:
            pass  # Suppress this expected exception and return without error (normally).

    async def _run_tasks_loop(self):
        metric_executor_state.state("running")
        self._startup_probe_handler.set_ready()
        while not self._is_shutdown:
            try:
                async for task in self._task_fetcher.run():
                    metric_tasks_fetched.inc()
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
            output = TaskOutput.internal_error(task)
            logger.error("task execution failed", exc_info=e)

        with (
            metric_tasks_reporting_outcome.track_inprogress(),
            metric_task_outcome_report_latency.time(),
        ):
            metric_task_outcome_reports.inc()
            await self._report_task_outcome(output=output, logger=logger)

        metric_task_completion_latency.observe(time.monotonic() - start_time)

    async def _run_task_and_get_output(self, task: Task, logger: Any) -> TaskOutput:
        graph: SerializedObject = await self._downloader.download_graph(task)
        input: SerializedObject = await self._downloader.download_input(task)
        init_value: Optional[SerializedObject] = (
            None
            if task.reducer_output_id is None
            else (await self._downloader.download_init_value(task))
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
        self._logger.info("shutting down")
        metric_executor_state.state("shutting_down")
        # There will be lots of task cancellation exceptions and "X is shutting down"
        # exceptions logged during Executor shutdown. Suppress their logs as they are
        # expected and are confusing for users.
        suppress_logging()

        self._is_shutdown = True
        await self._monitoring_server.shutdown()
        await self._task_runner.shutdown()
        await self._function_executor_states.shutdown()
        # We mainly need to cancel the task that runs _run_tasks_loop().
        for task in asyncio.all_tasks(loop):
            task.cancel()

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
