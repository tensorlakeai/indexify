import asyncio
import json
import ssl
from concurrent.futures.process import BrokenProcessPool
from typing import Dict, List, Optional

import httpx
import yaml
from httpx_sse import aconnect_sse
from pydantic import BaseModel
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.theme import Theme

from indexify.functions_sdk.data_objects import (
    FunctionWorkerOutput,
    IndexifyData,
    RouterOutput,
)

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
        code_path: str,
        function_worker: FunctionWorker,
        server_addr: str = "localhost:8900",
        config_path: Optional[str] = None,
    ):
        self.num_workers = num_workers
        self._use_tls = False
        if config_path:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
                self._config = config
            if config.get("use_tls", False):
                console.print(
                    "Running the extractor with TLS enabled", style="cyan bold"
                )
                self._use_tls = True
                tls_config = config["tls_config"]
                self._ssl_context = ssl.create_default_context(
                    ssl.Purpose.SERVER_AUTH, cafile=tls_config["ca_bundle_path"]
                )
                self._ssl_context.load_cert_chain(
                    certfile=tls_config["cert_path"], keyfile=tls_config["key_path"]
                )
                self._protocol = "wss"
                self._tls_config = tls_config
            else:
                self._ssl_context = None
                self._protocol = "ws"
        else:
            self._ssl_context = None
            self._protocol = "http"
            self._config = {}

        self._task_store: TaskStore = TaskStore()
        self._executor_id = executor_id
        self._function_worker = function_worker
        self._has_registered = False
        self._server_addr = server_addr
        self._base_url = f"{self._protocol}://{self._server_addr}"
        self._code_path = code_path
        self._downloader = Downloader(code_path=code_path, base_url=self._base_url)
        self._max_queued_tasks = 10
        self._task_reporter = TaskReporter(
            base_url=self._base_url, executor_id=self._executor_id
        )
        self._probe = RuntimeProbes()

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
                        f"Reporting outcome of task {task_outcome.task.id}\n"
                        f"Outcome: {style_outcome}\n"
                        f"Outputs: {len(task_outcome.outputs or [])}  Router Output: {task_outcome.router_output}",
                        title="Task Completion",
                        border_style="info",
                    )
                )

                try:
                    # Send task outcome to the server
                    await self._task_reporter.report_task_outcome(completed_task=task_outcome)
                except Exception as e:
                    self._handle_reporting_error(task_outcome, e)

    def _handle_reporting_error(self, task_outcome, exception: Exception):
        console.print(
            Panel(
                f"Failed to report task {task_outcome.task.id}\n"
                f"Exception: {exception}\nRetrying...",
                title="Reporting Error",
                border_style="error",
            )
        )
        await asyncio.sleep(5)

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
                    await self._handle_get_runnable_tasks(async_task)
                elif async_task.get_name() == "download_graph":
                    await self._handle_download_graph(async_task)
                elif async_task.get_name() == "download_input":
                    await self._handle_download_input(async_task)
                elif async_task.get_name() == "run_function":
                    await self._handle_run_function(async_task)

    async def _handle_get_runnable_tasks(self, async_task):
        # ... existing code ...

    async def _handle_download_graph(self, async_task):
        # ... existing code ...

    async def _handle_download_input(self, async_task):
        # ... existing code ...

    async def _handle_run_function(self, async_task):
        # ... existing code ...

    async def run(self):
        import signal

        asyncio.get_event_loop().add_signal_handler(
            signal.SIGINT, self.shutdown, asyncio.get_event_loop()
        )
        asyncio.create_task(self.task_launcher())
        asyncio.create_task(self.task_completion_reporter())
        self._should_run = True
        while self._should_run:
            self._protocol = "http"
            url = f"{self._protocol}://{self._server_addr}/internal/executors/{self._executor_id}/tasks"

            def to_sentence_case(snake_str):
                words = snake_str.split("_")
                return words[0].capitalize() + "" + " ".join(words[1:])

            runtime_probe: ProbeInfo = self._probe.probe()

            # Inspect the image
            if runtime_probe.is_default_executor:
                # install dependencies
                # rewrite the image name
                pass

            data = ExecutorMetadata(
                id=self._executor_id,
                addr="",
                image_name=runtime_probe.image_name,
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
                await self._register_executor(url, data)
            except Exception as e:
                console.print(
                    Text("registration Error: ", style="red bold")
                    + Text(f"failed to register: {e}", style="red")
                )
                await asyncio.sleep(5)
                continue

    async def _register_executor(self, url: str, data: Dict):
        async with httpx.AsyncClient() as client:
            async with aconnect_sse(
                client,
                "POST",
                url,
                json=data,
                headers={"Content-Type": "application/json"},
            ) as event_source:
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

    async def _shutdown(self, loop):
        console.print(Text("shutting down agent...", style="bold yellow"))
        self._should_run = False
        for task in asyncio.all_tasks(loop):
            task.cancel()

    def shutdown(self, loop):
        self._function_worker.shutdown()
        loop.create_task(self._shutdown(loop))
