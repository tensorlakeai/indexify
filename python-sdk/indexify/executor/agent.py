import asyncio
import json
import traceback
from concurrent.futures.process import BrokenProcessPool
from importlib.metadata import version
from pathlib import Path
from typing import Dict, List, Optional

from httpx_sse import aconnect_sse
from pydantic import BaseModel
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.theme import Theme

from indexify.common_util import get_httpx_client
from indexify.functions_sdk.data_objects import (
    FunctionWorkerOutput,
    IndexifyData,
)
from indexify.functions_sdk.graph_definition import ComputeGraphMetadata
from indexify.http_client import IndexifyClient

from ..functions_sdk.image import ImageInformation
from . import image_dependency_installer
from .api_objects import ExecutorMetadata, Task
from .downloader import DownloadedInputs, Downloader
from .executor_tasks import DownloadGraphTask, DownloadInputTask, ExtractTask
from .function_worker import FunctionWorker
from .runtime_probes import ProbeInfo, RuntimeProbes
from .task_reporter import TaskReporter
from .task_store import CompletedTask, TaskStore

custom_theme = Theme(
    {
        "info": "cyan",
        "warning": "yellow",
        "error": "red",
        "success": "green",
    }
)

console = Console(theme=custom_theme)


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

        runtime_probe: ProbeInfo = self._probe.probe()
        self._require_image_bootstrap = (
            True
            if (runtime_probe.is_default_executor and self.name_alias is not None)
            else False
        )
        self._executor_bootstrap_failed = False

        console.print(
            f"Require Bootstrap? {self._require_image_bootstrap}", style="cyan bold"
        )

        self.num_workers = num_workers
        if config_path:
            console.print("Running the extractor with TLS enabled", style="cyan bold")
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
        self._downloader = Downloader(code_path=code_path, base_url=self._base_url)
        self._max_queued_tasks = 10
        self._task_reporter = TaskReporter(
            base_url=self._base_url,
            executor_id=self._executor_id,
            config_path=self._config_path,
        )

    async def task_completion_reporter(self):
        console.print(Text("Starting task completion reporter", style="bold cyan"))
        # We should copy only the keys and not the values
        url = f"{self._protocol}://{self._server_addr}/write_content"
        while True:
            outcomes = await self._task_store.task_outcomes()
            for task_outcome in outcomes:
                outcome = task_outcome.task_outcome
                style_outcome = (
                    f"[bold red] {outcome} [/]"
                    if "fail" in outcome
                    else f"[bold green] {outcome} [/]"
                )
                console.print(
                    Panel(
                        f"Reporting outcome of task: {task_outcome.task.id}, function: {task_outcome.task.compute_fn}\n"
                        f"Outcome: {style_outcome}\n"
                        f"Num Fn Outputs: {len(task_outcome.outputs or [])}  Router Output: {task_outcome.router_output}",
                        title="Task Completion",
                        border_style="info",
                    )
                )

                try:
                    # Send task outcome to the server
                    self._task_reporter.report_task_outcome(completed_task=task_outcome)
                except Exception as e:
                    # The connection was dropped in the middle of the reporting, process, retry
                    console.print(
                        Panel(
                            f"Failed to report task {task_outcome.task.id}\n"
                            f"Exception: {e}\nRetrying...",
                            title="Reporting Error",
                            border_style="error",
                        )
                    )
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

                if self._executor_bootstrap_failed:
                    completed_task = CompletedTask(
                        task=task,
                        outputs=[],
                        task_outcome="failure",
                    )
                    self._task_store.complete(outcome=completed_task)

                    continue

                # Bootstrap this executor. Fail the task if we can't.
                if self._require_image_bootstrap:
                    try:
                        image_info = await _get_image_info_for_compute_graph(
                            task, self._protocol, self._server_addr, self._config_path
                        )
                        image_dependency_installer.executor_image_builder(
                            image_info, self.name_alias, self.image_version
                        )
                        self._require_image_bootstrap = False
                    except Exception as e:
                        console.print(
                            Text("Failed to bootstrap the executor ", style="red bold")
                            + Text(f"Exception: {traceback.format_exc()}", style="red")
                        )

                        self._executor_bootstrap_failed = True

                        completed_task = CompletedTask(
                            task=task,
                            outputs=[],
                            task_outcome="failure",
                        )
                        self._task_store.complete(outcome=completed_task)

                        continue

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
                        console.print(
                            Text("Task Launcher Error: ", style="red bold")
                            + Text(
                                f"Failed to get runnable tasks: {async_task.exception()}",
                                style="red",
                            )
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
                        console.print(
                            Text(
                                f"Failed to download graph for task {async_task.task.id}\n",
                                style="red bold",
                            )
                            + Text(f"Exception: {async_task.exception()}", style="red")
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
                        console.print(
                            Text(
                                f"Failed to download input for task {async_task.task.id}\n",
                                style="red bold",
                            )
                            + Text(f"Exception: {async_task.exception()}", style="red")
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
                        console.print(
                            Text(
                                f"Failed to execute task {async_task.task.id}\n",
                                style="red bold",
                            )
                            + Text(f"Exception: {e}", style="red")
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
            print(f"calling url: {url}")

            def to_sentence_case(snake_str):
                words = snake_str.split("_")
                return words[0].capitalize() + "" + " ".join(words[1:])

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

            panel_content = "\n".join(
                [f"{to_sentence_case(key)}: {value}" for key, value in data.items()]
            )
            console.print(
                Panel(
                    panel_content,
                    title="attempting to Register Executor",
                    border_style="cyan",
                )
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
                            resp = await event_source.response.aread().decode("utf-8")
                            console.print(f"failed to register: {str(resp)}")
                            await asyncio.sleep(5)
                            continue
                        console.print(
                            Text("executor registered successfully", style="bold green")
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
                console.print(
                    Text("registration Error: ", style="red bold")
                    + Text(f"failed to register: {e}", style="red")
                )
                await asyncio.sleep(5)
                continue

    async def _shutdown(self, loop):
        console.print(Text("shutting down agent...", style="bold yellow"))
        self._should_run = False
        for task in asyncio.all_tasks(loop):
            task.cancel()

    def shutdown(self, loop):
        self._function_worker.shutdown()
        loop.create_task(self._shutdown(loop))


async def _get_image_info_for_compute_graph(
    task: Task, protocol, server_addr, config_path: str
) -> ImageInformation:
    namespace = task.namespace
    graph_name: str = task.compute_graph
    compute_fn_name: str = task.compute_fn

    http_client = IndexifyClient(
        service_url=f"{protocol}://{server_addr}",
        namespace=namespace,
        config_path=config_path,
    )
    compute_graph: ComputeGraphMetadata = http_client.graph(graph_name)

    console.print(
        Text(
            f"Compute_fn name {compute_fn_name}, ComputeGraph {compute_graph} \n",
            style="red yellow",
        )
    )

    return compute_graph.nodes[compute_fn_name].compute_fn.image_information
