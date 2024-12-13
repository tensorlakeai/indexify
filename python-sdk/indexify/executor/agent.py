import asyncio
from pathlib import Path
from typing import Dict, List, Optional

import structlog

from .downloader import Downloader
from .executor_tasks import DownloadGraphTask, DownloadInputsTask, RunTask
from .function_executor.process_function_executor_factory import (
    ProcessFunctionExecutorFactory,
)
from .function_worker import (
    FunctionWorker,
    FunctionWorkerInput,
    FunctionWorkerOutput,
)
from .task_fetcher import TaskFetcher
from .task_reporter import TaskReporter
from .task_store import CompletedTask, TaskStore

logger = structlog.get_logger(module=__name__)


class ExtractorAgent:
    def __init__(
        self,
        executor_id: str,
        code_path: Path,
        server_addr: str = "localhost:8900",
        development_mode: bool = False,
        config_path: Optional[str] = None,
        name_alias: Optional[str] = None,
        image_hash: Optional[str] = None,
    ):
        self._config_path = config_path
        protocol: str = "http"
        if config_path:
            logger.info("running the extractor with TLS enabled")
            protocol = "https"

        self._task_store: TaskStore = TaskStore()
        self._function_worker = FunctionWorker(
            function_executor_factory=ProcessFunctionExecutorFactory(
                indexify_server_address=server_addr,
                development_mode=development_mode,
                config_path=config_path,
            )
        )
        self._has_registered = False
        self._server_addr = server_addr
        self._base_url = f"{protocol}://{self._server_addr}"
        self._code_path = code_path
        self._downloader = Downloader(
            code_path=code_path, base_url=self._base_url, config_path=config_path
        )
        self._task_fetcher = TaskFetcher(
            protocol=protocol,
            indexify_server_addr=self._server_addr,
            executor_id=executor_id,
            name_alias=name_alias,
            image_hash=image_hash,
            config_path=config_path,
        )
        self._task_reporter = TaskReporter(
            base_url=self._base_url,
            executor_id=executor_id,
            config_path=self._config_path,
        )

    async def task_completion_reporter(self):
        logger.info("starting task completion reporter")
        # We should copy only the keys and not the values
        while True:
            outcomes = await self._task_store.task_outcomes()
            for task_outcome in outcomes:
                logger.info(
                    "reporting_task_outcome",
                    task_id=task_outcome.task.id,
                    fn_name=task_outcome.task.compute_fn,
                    num_outputs=(
                        len(task_outcome.function_output.outputs)
                        if task_outcome.function_output is not None
                        else 0
                    ),
                    router_output=task_outcome.router_output,
                    outcome=task_outcome.task_outcome,
                    retries=task_outcome.reporting_retries,
                )

                try:
                    # Send task outcome to the server
                    self._task_reporter.report_task_outcome(completed_task=task_outcome)
                except Exception as e:
                    # The connection was dropped in the middle of the reporting, process, retry
                    logger.error(
                        "failed_to_report_task",
                        task_id=task_outcome.task.id,
                        exc_info=e,
                        retries=task_outcome.reporting_retries,
                    )
                    task_outcome.reporting_retries += 1
                    await asyncio.sleep(5)
                    continue

                self._task_store.mark_reported(task_id=task_outcome.task.id)

    async def task_launcher(self):
        async_tasks: List[asyncio.Task] = [
            asyncio.create_task(
                self._task_store.get_runnable_tasks(), name="get_runnable_tasks"
            )
        ]

        while True:
            done, pending = await asyncio.wait(
                async_tasks, return_when=asyncio.FIRST_COMPLETED
            )

            async_tasks: List[asyncio.Task] = list(pending)
            for async_task in done:
                if async_task.get_name() == "get_runnable_tasks":
                    if async_task.exception():
                        logger.error(
                            "task_launcher_error, failed to get runnable tasks",
                            exc_info=async_task.exception(),
                        )
                        continue
                    result: Dict[str, Task] = await async_task
                    task: Task
                    for _, task in result.items():
                        async_tasks.append(
                            DownloadGraphTask(
                                function_worker_input=FunctionWorkerInput(task=task),
                                downloader=self._downloader,
                            )
                        )
                    async_tasks.append(
                        asyncio.create_task(
                            self._task_store.get_runnable_tasks(),
                            name="get_runnable_tasks",
                        )
                    )
                elif async_task.get_name() == "download_graph":
                    if async_task.exception():
                        logger.error(
                            "task_launcher_error, failed to download graph",
                            exc_info=async_task.exception(),
                        )
                        completed_task = CompletedTask(
                            task=async_task.function_worker_input.task,
                            task_outcome="failure",
                        )
                        self._task_store.complete(outcome=completed_task)
                        continue
                    async_task: DownloadGraphTask
                    function_worker_input: FunctionWorkerInput = (
                        async_task.function_worker_input
                    )
                    function_worker_input.graph = await async_task
                    async_tasks.append(
                        DownloadInputsTask(
                            function_worker_input=function_worker_input,
                            downloader=self._downloader,
                        )
                    )
                elif async_task.get_name() == "download_inputs":
                    if async_task.exception():
                        logger.error(
                            "task_launcher_error, failed to download inputs",
                            exc_info=async_task.exception(),
                        )
                        completed_task = CompletedTask(
                            task=async_task.function_worker_input.task,
                            task_outcome="failure",
                        )
                        self._task_store.complete(outcome=completed_task)
                        continue
                    async_task: DownloadInputsTask
                    function_worker_input: FunctionWorkerInput = (
                        async_task.function_worker_input
                    )
                    function_worker_input.function_input = await async_task
                    async_tasks.append(
                        RunTask(
                            function_worker=self._function_worker,
                            function_worker_input=function_worker_input,
                        )
                    )
                elif async_task.get_name() == "run_task":
                    if async_task.exception():
                        completed_task = CompletedTask(
                            task=async_task.function_worker_input.task,
                            task_outcome="failure",
                            stderr=str(async_task.exception()),
                        )
                        self._task_store.complete(outcome=completed_task)
                        continue
                    async_task: RunTask
                    try:
                        outputs: FunctionWorkerOutput = await async_task
                        if not outputs.success:
                            task_outcome = "failure"
                        else:
                            task_outcome = "success"

                        completed_task = CompletedTask(
                            task=async_task.function_worker_input.task,
                            task_outcome=task_outcome,
                            function_output=outputs.function_output,
                            router_output=outputs.router_output,
                            stdout=outputs.stdout,
                            stderr=outputs.stderr,
                            reducer=outputs.reducer,
                        )
                        self._task_store.complete(outcome=completed_task)
                    except Exception as e:
                        logger.error(
                            "failed to execute task",
                            task_id=async_task.function_worker_input.task.id,
                            exc_info=e,
                        )
                        completed_task = CompletedTask(
                            task=async_task.function_worker_input.task,
                            task_outcome="failure",
                        )
                        self._task_store.complete(outcome=completed_task)
                        continue

    async def _main_loop(self):
        """Fetches incoming tasks from the server and starts their processing."""
        self._should_run = True
        while self._should_run:
            try:
                async for task in self._task_fetcher.run():
                    self._task_store.add_tasks([task])
            except Exception as e:
                logger.error("failed fetching tasks, retrying in 5 seconds", exc_info=e)
                await asyncio.sleep(5)
                continue

    async def run(self):
        import signal

        asyncio.get_event_loop().add_signal_handler(
            signal.SIGINT, self.shutdown, asyncio.get_event_loop()
        )
        asyncio.get_event_loop().add_signal_handler(
            signal.SIGTERM, self.shutdown, asyncio.get_event_loop()
        )
        asyncio.create_task(self.task_launcher())
        asyncio.create_task(self.task_completion_reporter())
        await self._main_loop()

    async def _shutdown(self, loop):
        logger.info("shutting_down")
        self._should_run = False
        await self._function_worker.shutdown()
        for task in asyncio.all_tasks(loop):
            task.cancel()

    def shutdown(self, loop):
        loop.create_task(self._shutdown(loop))
