import asyncio
import json
import ssl
from concurrent.futures.process import BrokenProcessPool
from typing import Dict, List, Optional, Union
import httpx
from httpx_sse import aconnect_sse

import yaml
from pydantic import BaseModel, Json

from .task_store import CompletedTask, TaskStore
from .api_objects import ExecutorMetadata, Task
from .content_downloader import Downloader
from .executor_tasks import DownloadInputTask, DownloadGraphTask, ExtractTask
from indexify.functions_sdk.data_objects import BaseData
from .function_worker import FunctionWorker
class FunctionInput(BaseModel):
    compute_graph: str
    function: str
    # Contains inputs of various tasks which want to invoke this function
    # key -> task_id, value -> json encoded input
    inputs: Dict[str, Json]

class FunctionState(BaseModel):
    # Number of tasks queued for this function
    # in the function worker
    tasks_queued: int = 0
    new_inputs: List[FunctionInput]

    @classmethod
    def new(cls, compute_graph: str, function: str) -> "FunctionState":
        return FunctionState(new_inputs=[FunctionInput(compute_graph=compute_graph, function=function, inputs={})])

class ExtractorAgent:
    def __init__(
        self,
        executor_id: str,
        num_workers,
        function_worker: FunctionWorker,
        server_addr: str = "localhost:8900",
        config_path: Optional[str] = None,
        code_path: str = "~/.indexify/code/"
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


    async def task_completion_reporter(self):
        print("starting task completion reporter")
        # We should copy only the keys and not the values
        url = f"{self._protocol}://{self._server_addr}/write_content"
        while True:
            outcomes = await self._task_store.task_outcomes()
            for task_outcome in outcomes:
                print(
                    f"reporting outcome of task {task_outcome.task_id}, outcome: {task_outcome.task_outcome}, outputs: {len(task_outcome.outputs)}"
                )
                task: Task = self._task_store.get_task(task_outcome.task_id)
                try:
                    # Send task outcome to the server 
                    pass
                except Exception as e:
                    # the connection was dropped in the middle of the reporting process, retry
                    print(
                        f"failed to report task {task_outcome.task_id}, exception: {e}, retrying"
                    )
                    continue

                self._task_store.mark_reported(task_id=task_outcome.task_id)

    async def task_launcher(self):
        async_tasks: List[asyncio.Task] = []
        function_states: Dict[str, FunctionState] = {}
        async_tasks.append(
            asyncio.create_task(
                self._task_store.get_runnable_tasks(), name="get_runnable_tasks"
            )
        )
        while True:
            state: FunctionState
            for _, state in function_states.items():
                print(f"state: {state}")
                if (
                    state.tasks_queued == 0
                    and len(state.new_inputs[0].inputs) != 0
                ):
                    fn_input = state.new_inputs.pop(0)
                    print(
                        f"running {fn_input.function} with {len(fn_input.inputs)} inputs from tasks"
                    )
                    for task_id, input in fn_input.inputs.items():
                        task: Task = self._task_store.get_task(task_id)
                        async_tasks.append(
                            ExtractTask(
                                function_worker=self._function_worker,
                                task=task,
                                input=input,
                                code_path=self._code_path,
                            )
                        )
                    state.tasks_queued += 1

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
                            f"failed to download content {async_task.exception()} for task {async_task.task_id}"
                        )
                        completed_task = CompletedTask(
                            task_id=async_task.task_id,
                            outputs=[],
                            task_outcome="Failed",
                        )
                        self._task_store.complete(outcome=completed_task)
                        continue
                    async_tasks.append(
                        DownloadInputTask(task=async_task.task, downloader=self._downloader)
                    )
                elif async_task.get_name() == "download_input":
                    if async_task.exception():
                        print(
                            f"failed to download content {async_task.exception()} for task {async_task.task_id}"
                        )
                        completed_task = CompletedTask(
                            task_id=async_task.task_id,
                            outputs=[],
                            task_outcome="Failed",
                        )
                        self._task_store.complete(outcome=completed_task)
                        continue
                    # Process all completed downloads and accumulate them in batches
                    # without creating extraction tasks right away.
                    function_input: Json
                    function_input = await async_task
                    task: Task = self._task_store.get_task(async_task.task_id)
                    state: FunctionState = function_states.setdefault(
                        f"{task.namespace}/{task.compute_graph}/{task.compute_fn}", FunctionState.new(task.compute_graph, task.compute_fn)
                    )
                    if len(state.new_inputs[-1].inputs) == self._max_queued_tasks:
                        state.new_inputs.append(FunctionInput(compute_graph=task.compute_graph, function=task.compute_fn))
                    fn_input = state.new_inputs[-1]
                    fn_input.inputs[task.id] = function_input
                elif async_task.get_name() == "run_function":
                    async_task: ExtractTask
                    state: FunctionState = function_states[f"{async_task.namespace}/{async_task.compute_graph}/{async_task.compute_fn}"]
                    state.tasks_queued -= 1
                    try:
                        outputs = await async_task
                        print(f"completed task {async_task.task_id}")
                        completed_task = CompletedTask(
                            task_id=async_task.task_id,
                            task_outcome="Success",
                            outputs=outputs,
                        )
                        self._task_store.complete(outcome=completed_task)
                    except BrokenProcessPool:
                        self._task_store.retriable_failure(async_task.task_id)
                        continue
                    except Exception as e:
                        print(f"failed to execute tasks {async_task.task_id} {e}")
                        completed_task = CompletedTask(
                            task_id=async_task.task_id,
                            task_outcome="Failed",
                            outputs=[],
                            features=[],
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
            print(data)
            print("attempting to register")
            try:
                async with httpx.AsyncClient() as client:
                    async with aconnect_sse(client, "POST", url, json=data, headers={"Content-Type": "application/json"}) as event_source: # type: ignore
                        async for sse in event_source.aiter_sse():
                            data = json.loads(sse.data)
                            tasks = []
                            for task_dict in data:
                                tasks.append(Task.model_validate(task_dict, strict=False))
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
