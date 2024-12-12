import asyncio
import json
from importlib.metadata import version
from pathlib import Path
from typing import Dict, List, Optional

import structlog
from httpx_sse import aconnect_sse

from indexify.common_util import get_httpx_client

from .api_objects import ExecutorMetadata, Task
from .downloader import DownloadedInputs, Downloader
from .executor_tasks import DownloadGraphTask, DownloadInputsTask, RunTask
from .function_executor.process_function_executor_factory import (
    ProcessFunctionExecutorFactory,
)
from .function_worker import (
    FunctionWorker,
    FunctionWorkerInput,
    FunctionWorkerOutput,
)
from .runtime_probes import ProbeInfo, RuntimeProbes
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
        image_version: Optional[int] = None,
    ):
        self.name_alias = name_alias
        self.image_version = image_version
        self._config_path = config_path
        self._probe = RuntimeProbes()

        if config_path:
            logger.info("running the extractor with TLS enabled")
            self._protocol = "https"
        else:
            self._protocol = "http"

        self._task_store: TaskStore = TaskStore()
        self._executor_id = executor_id
        self._function_worker = FunctionWorker(
            function_executor_factory=ProcessFunctionExecutorFactory(
                indexify_server_address=server_addr,
                development_mode=development_mode,
                config_path=config_path,
            )
        )
        self._has_registered = False
        self._server_addr = server_addr
        self._base_url = f"{self._protocol}://{self._server_addr}"
        self._code_path = code_path
        self._downloader = Downloader(
            code_path=code_path, base_url=self._base_url, config_path=config_path
        )
        self._task_reporter = TaskReporter(
            base_url=self._base_url,
            executor_id=self._executor_id,
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
        self._should_run = True
        while self._should_run:
            url = f"{self._protocol}://{self._server_addr}/internal/executors/{self._executor_id}/tasks"
            runtime_probe: ProbeInfo = self._probe.probe()

            executor_version = version("indexify")

            image_name = (
                self.name_alias
                if self.name_alias is not None
                else runtime_probe.image_name
            )

            image_version: int = (
                self.image_version
                if self.image_version is not None
                else runtime_probe.image_version
            )

            data = ExecutorMetadata(
                id=self._executor_id,
                executor_version=executor_version,
                addr="",
                image_name=image_name,
                image_version=image_version,
                labels=runtime_probe.labels,
            ).model_dump()
            logger.info(
                "registering_executor",
                executor_id=self._executor_id,
                url=url,
                executor_version=executor_version,
            )
            try:
                async with get_httpx_client(self._config_path, True) as client:
                    async with aconnect_sse(
                        client,
                        "POST",
                        url,
                        json=data,
                        headers={"Content-Type": "application/json"},
                    ) as event_source:
                        if not event_source.response.is_success:
                            resp = await event_source.response.aread()
                            logger.error(
                                f"failed to register",
                                resp=str(resp),
                                status_code=event_source.response.status_code,
                            )
                            await asyncio.sleep(5)
                            continue
                        logger.info(
                            "executor_registered", executor_id=self._executor_id
                        )
                        async for sse in event_source.aiter_sse():
                            data = json.loads(sse.data)
                            tasks = []
                            for task_dict in data:
                                tasks.append(
                                    Task.model_validate(task_dict, strict=False)
                                )
                            self._task_store.add_tasks(tasks)
            except Exception as e:
                logger.error("failed to register", exc_info=e)
                await asyncio.sleep(5)
                continue

    async def _shutdown(self, loop):
        logger.info("shutting_down")
        self._should_run = False
        await self._function_worker.shutdown()
        for task in asyncio.all_tasks(loop):
            task.cancel()

    def shutdown(self, loop):
        loop.create_task(self._shutdown(loop))
