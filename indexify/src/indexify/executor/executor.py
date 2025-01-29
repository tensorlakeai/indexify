import asyncio
import signal
from pathlib import Path
from typing import Any, List, Optional

import structlog
from tensorlake.function_executor.proto.function_executor_pb2 import SerializedObject
from tensorlake.utils.logging import suppress as suppress_logging

from .api_objects import FunctionURI, Task
from .downloader import Downloader
from .function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerFactory,
)
from .task_fetcher import TaskFetcher
from .task_reporter import TaskReporter
from .task_runner import TaskInput, TaskOutput, TaskRunner


class Executor:
    def __init__(
        self,
        id: str,
        version: str,
        code_path: Path,
        function_allowlist: Optional[List[FunctionURI]],
        function_executor_server_factory: FunctionExecutorServerFactory,
        server_addr: str = "localhost:8900",
        config_path: Optional[str] = None,
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
        self._task_runner = TaskRunner(
            function_executor_server_factory=function_executor_server_factory,
            base_url=self._base_url,
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

    def run(self):
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

        try:
            asyncio.get_event_loop().run_until_complete(self._run_async())
        except asyncio.CancelledError:
            pass  # Suppress this expected exception and return without error (normally).

    async def _run_async(self):
        while not self._is_shutdown:
            try:
                async for task in self._task_fetcher.run():
                    asyncio.create_task(self._run_task(task))
            except Exception as e:
                self._logger.error(
                    "failed fetching tasks, retrying in 5 seconds", exc_info=e
                )
                await asyncio.sleep(5)

    async def _run_task(self, task: Task) -> None:
        """Runs the supplied task.

        Doesn't raise any Exceptions. All errors are reported to the server."""
        logger = self._task_logger(task)
        output: Optional[TaskOutput] = None

        try:
            graph: SerializedObject = await self._downloader.download_graph(task)
            input: SerializedObject = await self._downloader.download_input(task)
            init_value: Optional[SerializedObject] = (
                await self._downloader.download_init_value(task)
            )
            logger.info("task_execution_started")
            output: TaskOutput = await self._task_runner.run(
                TaskInput(
                    task=task,
                    graph=graph,
                    input=input,
                    init_value=init_value,
                ),
                logger=logger,
            )
            logger.info("task_execution_finished", success=output.success)
        except Exception as e:
            output = TaskOutput.internal_error(task)
            logger.error("task_execution_failed", exc_info=e)

        await self._report_task_outcome(output=output, logger=logger)

    async def _report_task_outcome(self, output: TaskOutput, logger: Any) -> None:
        """Reports the task with the given output to the server."""
        reporting_retries: int = 0

        while True:
            logger = logger.bind(retries=reporting_retries)
            try:
                await self._task_reporter.report(output=output, logger=logger)
                break
            except Exception as e:
                logger.error(
                    "failed_to_report_task",
                    exc_info=e,
                )
                reporting_retries += 1
                await asyncio.sleep(5)

    async def _shutdown(self, loop):
        self._logger.info("shutting_down")
        # There will be lots of task cancellation exceptions and "X is shutting down"
        # exceptions logged during Executor shutdown. Suppress their logs as they are
        # expected and are confusing for users.
        suppress_logging()

        self._is_shutdown = True
        await self._task_runner.shutdown()
        # We mainly need to cancel the task that runs _run_async() loop.
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
