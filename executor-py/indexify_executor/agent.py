import asyncio
import json
import ssl
from concurrent.futures.process import BrokenProcessPool
from typing import Dict, List, Optional, Union

import httpx
import yaml
from httpx_sse import aconnect_sse
from indexify.functions_sdk.data_objects import BaseData, RouterOutput
from pydantic import BaseModel, Json

from .api_objects import ExecutorMetadata, Task
from .downloader import Downloader
from .executor_tasks import DownloadGraphTask, DownloadInputTask, ExtractTask
from .function_worker import FunctionWorker
from .task_reporter import TaskReporter
from .task_store import CompletedTask, TaskStore


class FunctionInput(BaseModel):
    task_id: str
    namespace: str
    compute_graph: str
    function: str
    input: bytes


class ExtractorAgent:
    def __init__(
        self,
        executor_id: str,
        num_workers,
        function_worker: FunctionWorker,
        server_addr: str = "localhost:8900",
        config_path: Optional[str] = None,
        code_path: str = "~/.indexify/code/",
    ):
        self.num_workers = num_workers
        self._use_tls = False
        if config_path:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
                self._config = config
            if config.get("use_tls", False):
                print("Running the extractor with TLS enabled")
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

    async def task_completion_reporter(self):
        print("starting task completion reporter")
        # We should copy only the keys and not the values
        url = f"{self._protocol}://{self._server_addr}/write_content"
        while True:
            outcomes = await self._task_store.task_outcomes()
            for task_outcome in outcomes:
                print(
                    f"reporting outcome of task {task_outcome.task.id}, outcome: {task_outcome.task_outcome}, outputs: {len(task_outcome.outputs)}"
                )
                task: Task = self._task_store.get_task(task_outcome.task.id)
                try:
                    # Send task outcome to the server
                    self._task_reporter.report_task_outcome(
                        task_outcome.outputs,
                        task_outcome.router_output,
                        task_outcome.task,
                        task_outcome.task_outcome,
                    )
                except Exception as e:
                    # the connection was dropped in the middle of the reporting process, retry
                    print(
                        f"failed to report task {task_outcome.task.id}, exception: {e}, retrying"
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
                async_tasks.append(
                    ExtractTask(
                        function_worker=self._function_worker,
                        task=task,
                        input=fn.input,
                        code_path=f"{self._code_path}/{task.namespace}/{task.compute_graph}",
                    )
                )
            fn_queue = []
            done, pending = await asyncio.wait(
                async_tasks, return_when=asyncio.FIRST_COMPLETED
            )
            async_tasks: List[asyncio.Task] = list(pending)
            for async_task in done:
                if async_task.get_name() == "get_runnable_tasks":
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
                        print(
                            f"failed to download graph {async_task.exception()} for task {async_task.task.id}"
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
                        print(
                            f"failed to download input {async_task.exception()} for task {async_task.task.id}"
                        )
                        completed_task = CompletedTask(
                            task=async_task.task,
                            outputs=[],
                            task_outcome="failure",
                        )
                        self._task_store.complete(outcome=completed_task)
                        continue
                    function_input: bytes = await async_task
                    task: Task = async_task.task
                    fn_queue.append(
                        FunctionInput(
                            task_id=task.id,
                            namespace=task.namespace,
                            compute_graph=task.compute_graph,
                            function=task.compute_fn,
                            input=function_input,
                        )
                    )
                elif async_task.get_name() == "run_function":
                    async_task: ExtractTask
                    try:
                        outputs = await async_task
                        router_output = outputs if isinstance(outputs, RouterOutput) else None
                        fn_outputs = outputs if not isinstance(outputs, RouterOutput) else []
                        completed_task = CompletedTask(
                            task=async_task.task,
                            task_outcome="success",
                            outputs=fn_outputs,
                            router_output=router_output,
                        )
                        self._task_store.complete(outcome=completed_task)
                    except BrokenProcessPool:
                        self._task_store.retriable_failure(async_task.task.id)
                        continue
                    except Exception as e:
                        print(f"failed to execute tasks {async_task.task.id} {e}")
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
            self._protocol = "http"
            url = f"{self._protocol}://{self._server_addr}/internal/executors/{self._executor_id}/tasks"
            print(url)
            data = ExecutorMetadata(
                id=self._executor_id,
                address="",
                runner_name="extractor",
                labels={},
            ).model_dump()
            print(f"attempting to register executor: {data}")
            try:
                async with httpx.AsyncClient() as client:
                    async with aconnect_sse(client, "POST", url, json=data, headers={"Content-Type": "application/json"}) as event_source:  # type: ignore
                        async for sse in event_source.aiter_sse():
                            data = json.loads(sse.data)
                            tasks = []
                            for task_dict in data:
                                tasks.append(
                                    Task.model_validate(task_dict, strict=False)
                                )
                            self._task_store.add_tasks(tasks)
            except Exception as e:
                print(f"failed to register: {e}")
                await asyncio.sleep(5)
                continue

    async def _shutdown(self, loop):
        print("shutting down agent ...")
        self._should_run = False
        for task in asyncio.all_tasks(loop):
            task.cancel()

    def shutdown(self, loop):
        self._function_worker.shutdown()
        loop.create_task(self._shutdown(loop))
