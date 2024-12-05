import asyncio
import json
from concurrent.futures.process import BrokenProcessPool
from importlib.metadata import version
from pathlib import Path
from typing import Dict, List, Optional

import structlog
from httpx_sse import aconnect_sse
from pydantic import BaseModel

from indexify.common_util import get_httpx_client
from indexify.functions_sdk.data_objects import (
    FunctionWorkerOutput,
    IndexifyData,
)
from indexify.http_client import IndexifyClient

from .api_objects import ExecutorMetadata, Task
from .downloader import DownloadedInputs, Downloader
from .executor_tasks import DownloadGraphTask, DownloadInputTask, ExtractTask
from .function_worker import FunctionWorker
from .runtime_probes import ProbeInfo, RuntimeProbes
from .task_reporter import TaskReporter
from .task_store import CompletedTask, TaskStore

logging = structlog.get_logger(module=__name__)


class FunctionInput(BaseModel):
    task_id: str
    namespace: str
    compute_graph: str
    function: str
    input: IndexifyData
    init_value: Optional[IndexifyData] = None


class ExtractorAgent:
    def __init__(
        self,
        executor_id: str,
        num_workers,
        code_path: Path,
        server_addr: str = "localhost:8900",
        config_path: Optional[str] = None,
        name_alias: Optional[str] = None,
        image_version: Optional[int] = None,
    ):
        self.name_alias = name_alias
        self.image_version = image_version
        self._config_path = config_path
        self._probe = RuntimeProbes()

        self.num_workers = num_workers
        if config_path:
            logging.info("running the extractor with TLS enabled")
            self._protocol = "https"
        else:
            self._protocol = "http"

        self._task_store: TaskStore = TaskStore()
        self._executor_id = executor_id
        self._function_worker = FunctionWorker(
            workers=num_workers,
            indexify_client=IndexifyClient(
                service_url=f"{self._protocol}://{server_addr}",
                config_path=config_path,
            ),
        )
        self._has_registered = False
        self._server_addr = server_addr
        self._base_url = f"{self._protocol}://{self._server_addr}"
        self._code_path = code_path
        self._downloader = Downloader(
            code_path=code_path, base_url=self._base_url, config_path=self._config_path
        )
        self._max_queued_tasks = 10
        self._task_reporter = TaskReporter(
            base_url=self._base_url,
            executor_id=self._executor_id,
            config_path=self._config_path,
        )

    async def task_completion_reporter(self):
        logging.info("starting task completion reporter")
        # We should copy only the keys and not the values
        while True:
            outcomes = await self._task_store.task_outcomes()
            for task_outcome in outcomes:
                retryStr = (
                    f"\nRetries: {task_outcome.reporting_retries}"
                    if task_outcome.reporting_retries > 0
                    else ""
                )
                outcome = task_outcome.task_outcome
                style_outcome = (
                    f"[bold red] {outcome} [/]"
                    if "fail" in outcome
                    else f"[bold green] {outcome} [/]"
                )
                logging.info(
                    "reporting_task_outcome",
                    task_id=task_outcome.task.id,
                    fn_name=task_outcome.task.compute_fn,
                    num_outputs=len(task_outcome.outputs or []),
                    router_output=task_outcome.router_output,
                    outcome=task_outcome.task_outcome,
                    retries=task_outcome.reporting_retries,
                )

                try:
                    # Send task outcome to the server
                    self._task_reporter.report_task_outcome(completed_task=task_outcome)
                except Exception as e:
                    # The connection was dropped in the middle of the reporting, process, retry
                    logging.error(
                        "failed_to_report_task",
                        task_id=task_outcome.task.id,
                        exception=f"exception: {type(e).__name__}({e})",
                        retries=task_outcome.reporting_retries,
                    )
                    task_outcome.reporting_retries += 1
                    await asyncio.sleep(5)
                    continue

                self._task_store.mark_reported(task_id=task_outcome.task.id)

    async def task_launcher(self):
        async_tasks: List[asyncio.Task] = []
        fn_queue: List[FunctionInput] = []

        async_tasks.append(
            asyncio.create_task(
                self._task_store.get_runnable_tasks(), name="get_runnable_tasks"
            )
        )

        while True:
            fn: FunctionInput
            for fn in fn_queue:
                task: Task = self._task_store.get_task(fn.task_id)
                async_tasks.append(
                    ExtractTask(
                        function_worker=self._function_worker,
                        task=task,
                        input=fn.input,
                        code_path=f"{self._code_path}/{task.namespace}/{task.compute_graph}.{task.graph_version}",
                        init_value=fn.init_value,
                    )
                )

            fn_queue = []
            done, pending = await asyncio.wait(
                async_tasks, return_when=asyncio.FIRST_COMPLETED
            )

            async_tasks: List[asyncio.Task] = list(pending)
            for async_task in done:
                if async_task.get_name() == "get_runnable_tasks":
                    if async_task.exception():
                        logging.error(
                            "task_launcher_error, failed to get runnable tasks",
                            exception=async_task.exception(),
                        )
                        continue
                    result: Dict[str, Task] = await async_task
                    task: Task
                    for _, task in result.items():
                        async_tasks.append(
                            DownloadGraphTask(task=task, downloader=self._downloader)
                        )
                    async_tasks.append(
                        asyncio.create_task(
                            self._task_store.get_runnable_tasks(),
                            name="get_runnable_tasks",
                        )
                    )
                elif async_task.get_name() == "download_graph":
                    if async_task.exception():
                        logging.error(
                            "task_launcher_error, failed to download graph",
                            exception=async_task.exception(),
                        )
                        completed_task = CompletedTask(
                            task=async_task.task,
                            outputs=[],
                            task_outcome="failure",
                        )
                        self._task_store.complete(outcome=completed_task)
                        continue
                    async_tasks.append(
                        DownloadInputTask(
                            task=async_task.task, downloader=self._downloader
                        )
                    )
                elif async_task.get_name() == "download_input":
                    if async_task.exception():
                        logging.error(
                            "task_launcher_error, failed to download input",
                            exception=str(async_task.exception()),
                        )
                        completed_task = CompletedTask(
                            task=async_task.task,
                            outputs=[],
                            task_outcome="failure",
                        )
                        self._task_store.complete(outcome=completed_task)
                        continue
                    downloaded_inputs: DownloadedInputs = await async_task
                    task: Task = async_task.task
                    fn_queue.append(
                        FunctionInput(
                            task_id=task.id,
                            namespace=task.namespace,
                            compute_graph=task.compute_graph,
                            function=task.compute_fn,
                            input=downloaded_inputs.input,
                            init_value=downloaded_inputs.init_value,
                        )
                    )
                elif async_task.get_name() == "run_function":
                    if async_task.exception():
                        completed_task = CompletedTask(
                            task=async_task.task,
                            task_outcome="failure",
                            outputs=[],
                            stderr=str(async_task.exception()),
                        )
                        self._task_store.complete(outcome=completed_task)
                        continue
                    async_task: ExtractTask
                    try:
                        outputs: FunctionWorkerOutput = await async_task
                        if not outputs.success:
                            task_outcome = "failure"
                        else:
                            task_outcome = "success"

                        completed_task = CompletedTask(
                            task=async_task.task,
                            task_outcome=task_outcome,
                            outputs=outputs.fn_outputs,
                            router_output=outputs.router_output,
                            stdout=outputs.stdout,
                            stderr=outputs.stderr,
                            reducer=outputs.reducer,
                        )
                        self._task_store.complete(outcome=completed_task)
                    except BrokenProcessPool:
                        self._task_store.retriable_failure(async_task.task.id)
                        continue
                    except Exception as e:
                        logging.error(
                            "failed to execute task",
                            task_id=async_task.task.id,
                            exception=str(e),
                        )
                        completed_task = CompletedTask(
                            task=async_task.task,
                            task_outcome="failure",
                            outputs=[],
                        )
                        self._task_store.complete(outcome=completed_task)
                        continue

    async def run(self):
        import signal

        asyncio.get_event_loop().add_signal_handler(
            signal.SIGINT, self.shutdown, asyncio.get_event_loop()
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
            logging.info("registering_executor", executor_id=self._executor_id)
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
                            resp = await event_source.response
                            resp_content = resp.aread()
                            logging.error(
                                f"failed to register",
                                resp=str(resp_content),
                                status_code=event_source.response.status_code,
                            )
                            await asyncio.sleep(5)
                            continue
                        logging.info(
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
                logging.error(f"failed to register: {e}")
                await asyncio.sleep(5)
                continue

    async def _shutdown(self, loop):
        logging.info("shutting_down")
        self._should_run = False
        for task in asyncio.all_tasks(loop):
            task.cancel()

    def shutdown(self, loop):
        self._function_worker.shutdown()
        loop.create_task(self._shutdown(loop))
