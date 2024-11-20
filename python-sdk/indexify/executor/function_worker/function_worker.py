import multiprocessing as mp
import sys
import traceback
from asyncio import Future
from typing import Dict, List, Optional

import cloudpickle
from pydantic import BaseModel
from rich import print

from indexify import IndexifyClient
from indexify.executor.function_worker.function_worker_utils import (
    get_optimal_process_count,
)
from indexify.functions_sdk.data_objects import (
    FunctionWorkerOutput,
    IndexifyData,
    RouterOutput,
)
from indexify.functions_sdk.indexify_functions import (
    FunctionCallResult,
    GraphInvocationContext,
    IndexifyFunctionWrapper,
    RouterCallResult,
)

function_wrapper_map: Dict[str, IndexifyFunctionWrapper] = {}


class FunctionRunException(Exception):
    def __init__(
        self, exception: Exception, stdout: str, stderr: str, is_reducer: bool
    ):
        super().__init__(str(exception))
        self.exception = exception
        self.stdout = stdout
        self.stderr = stderr
        self.is_reducer = is_reducer


class FunctionOutput(BaseModel):
    fn_outputs: Optional[List[IndexifyData]]
    router_output: Optional[RouterOutput]
    reducer: bool = False
    success: bool = True
    stdout: str = ""
    stderr: str = ""


def _load_function(
    namespace: str,
    graph_name: str,
    fn_name: str,
    code_path: str,
    version: int,
    invocation_id: str,
    indexify_client: IndexifyClient,
):
    """Load an extractor to the memory: extractor_wrapper_map."""
    global function_wrapper_map
    key = f"{namespace}/{graph_name}/{version}/{fn_name}"
    if key in function_wrapper_map:
        return
    with open(code_path, "rb") as f:
        code = f.read()
        pickled_functions = cloudpickle.loads(code)
    context = GraphInvocationContext(
        invocation_id=invocation_id,
        graph_name=graph_name,
        graph_version=str(version),
        indexify_client=indexify_client,
    )
    function_wrapper = IndexifyFunctionWrapper(
        cloudpickle.loads(pickled_functions[fn_name]),
        context,
    )
    function_wrapper_map[key] = function_wrapper


class Job(BaseModel):
    namespace: str
    graph_name: str
    fn_name: str
    input: IndexifyData
    code_path: str
    version: int
    init_value: Optional[IndexifyData] = None
    invocation_id: Optional[str] = None


class FunctionWorker:
    def __init__(
        self,
        workers: int = get_optimal_process_count(),
        pool_size: int = 1000,
        indexify_client: IndexifyClient = None,
    ) -> None:
        self._workers: int = workers
        self._indexify_client: IndexifyClient = indexify_client
        self.job_queue: mp.Queue[tuple[Future, Job]] = mp.Queue(maxsize=pool_size)
        self.shutdown_event = mp.Event()
        self.running_processes: list[mp.Process] = []
        self.finished_jobs = mp.Queue()
        self._run()

    def _run(self):
        while not self.shutdown_event.is_set():
            if not self.job_queue.empty():
                if len(self.running_processes) < self._workers:
                    future, job = self.job_queue.get()
                    process = mp.Process(target=self._run_process, args=(future, job))
                    process.start()
                    self.running_processes.append(process)
                else:
                    # pass and wait for some processes to finish.
                    continue

    def _run_process(self, future: Future, job: Job):
        try:
            result = _run_function(
                job.namespace,
                job.graph_name,
                job.fn_name,
                job.input,
                job.code_path,
                job.version,
                job.init_value,
                job.invocation_id,
                self._indexify_client,
            )
            future.set_result(
                FunctionWorkerOutput(
                    fn_outputs=result.fn_outputs,
                    router_output=result.router_output,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    reducer=result.reducer,
                    success=result.success,
                )
            )
        except Exception as e:
            future.set_result(
                FunctionWorkerOutput(
                    stdout=e.stdout,
                    stderr=e.stderr,
                    reducer=e.is_reducer,
                    success=False,
                )
            )

    async def async_submit(
        self,
        namespace: str,
        graph_name: str,
        fn_name: str,
        input: IndexifyData,
        code_path: str,
        version: int,
        init_value: Optional[IndexifyData] = None,
        invocation_id: Optional[str] = None,
    ) -> Future:
        completion_future = Future()
        self.job_queue.put(
            (
                completion_future,
                Job(
                    namespace=namespace,
                    graph_name=graph_name,
                    fn_name=fn_name,
                    input=input,
                    code_path=code_path,
                    version=version,
                    init_value=init_value,
                    invocation_id=invocation_id,
                    indexify_client=self._indexify_client,
                ),
            )
        )
        return completion_future

    def shutdown(self):
        self.shutdown_event.set()
        # kill the processes when we receive shutdown signal
        print(
            f"[bold] function_worker: [/bold] Shutdown signal received, killing {len(self.running_processes)} processes"
        )
        for process in self.running_processes:
            process.kill()


def _run_function(
    namespace: str,
    graph_name: str,
    fn_name: str,
    input: IndexifyData,
    code_path: str,
    version: int,
    init_value: Optional[IndexifyData] = None,
    invocation_id: Optional[str] = None,
    indexify_client: Optional[IndexifyClient] = None,
) -> FunctionOutput:
    import io
    from contextlib import redirect_stderr, redirect_stdout

    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    is_reducer = False
    router_output = None
    fn_output = None
    has_failed = False
    print(
        f"[bold] function_worker: [/bold] invoking function {fn_name} in graph {graph_name}"
    )
    with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
        try:
            key = f"{namespace}/{graph_name}/{version}/{fn_name}"
            if key not in function_wrapper_map:
                _load_function(
                    namespace,
                    graph_name,
                    fn_name,
                    code_path,
                    version,
                    invocation_id,
                    indexify_client,
                )

            fn = function_wrapper_map[key]
            if (
                str(type(fn.indexify_function))
                == "<class 'indexify.functions_sdk.indexify_functions.IndexifyRouter'>"
            ):
                router_call_result: RouterCallResult = fn.invoke_router(fn_name, input)
                router_output = RouterOutput(edges=router_call_result.edges)
                if router_call_result.traceback_msg is not None:
                    print(router_call_result.traceback_msg, file=sys.stderr)
                    has_failed = True
            else:
                fn_call_result: FunctionCallResult = fn.invoke_fn_ser(
                    fn_name, input, init_value
                )
                is_reducer = fn.indexify_function.accumulate is not None
                fn_output = fn_call_result.ser_outputs
                if fn_call_result.traceback_msg is not None:
                    print(fn_call_result.traceback_msg, file=sys.stderr)
                    has_failed = True
        except Exception:
            print(traceback.format_exc(), file=sys.stderr)
            has_failed = True

    # WARNING - IF THIS FAILS, WE WILL NOT BE ABLE TO RECOVER
    # ANY LOGS
    if has_failed:
        return FunctionOutput(
            fn_outputs=None,
            router_output=None,
            stdout=stdout_capture.getvalue(),
            stderr=stderr_capture.getvalue(),
            reducer=is_reducer,
            success=False,
        )
    return FunctionOutput(
        fn_outputs=fn_output,
        router_output=router_output,
        reducer=is_reducer,
        success=True,
        stdout=stdout_capture.getvalue(),
        stderr=stderr_capture.getvalue(),
    )
