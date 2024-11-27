import asyncio
import sys
import traceback
from typing import List, Optional

from pydantic import BaseModel
from rich import print

from indexify import IndexifyClient
from indexify.executor.api_objects import Task
from indexify.executor.executor_tasks import RunFunctionTask
from indexify.executor.function_worker.function_worker_utils import (
    _load_function,
)
from indexify.functions_sdk.data_objects import (
    FunctionWorkerOutput,
    IndexifyData,
    RouterOutput,
)
from indexify.functions_sdk.indexify_functions import (
    FunctionCallResult,
    RouterCallResult,
)


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

class FunctionWorker:
    def __init__(
        self,
        indexify_client: IndexifyClient = None,
    ) -> None:
        self._indexify_client: IndexifyClient = indexify_client
        self._loop = asyncio.get_event_loop()

    def run_function(
        self,
        task: Task,
        fn_input: IndexifyData,
        init_value: IndexifyData | None,
        code_path: str,
    ):
        return RunFunctionTask(
            task=task,
            coroutine=self.async_submit(
                namespace=task.namespace,
                graph_name=task.compute_graph,
                fn_name=task.compute_fn,
                input=fn_input,
                init_value=init_value,
                code_path=code_path,
                version=task.graph_version,
                invocation_id=task.invocation_id,
            ),
            loop=self._loop,
        )

    async def async_submit(self, **kwargs) -> FunctionWorkerOutput:
        try:
            print(f"Submitting async function.....")
            result = await _run_function(
                kwargs["namespace"],
                kwargs["graph_name"],
                kwargs["fn_name"],
                kwargs["input"],
                kwargs["code_path"],
                kwargs["version"],
                kwargs["init_value"],
                kwargs["invocation_id"],
                self._indexify_client,
            )
            return FunctionWorkerOutput(
                fn_outputs=result.fn_outputs,
                router_output=result.router_output,
                stdout=result.stdout,
                stderr=result.stderr,
                reducer=result.reducer,
                success=result.success,
            )
        except Exception as e:
            print(e)
            return FunctionWorkerOutput(
                stdout=e.stdout,
                stderr=e.stderr,
                reducer=e.is_reducer,
                success=False,
            )


async def _run_function(
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
            fn = _load_function(
                namespace,
                graph_name,
                fn_name,
                code_path,
                version,
                invocation_id,
                indexify_client,
            )
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
                print(f"is function async: {fn.indexify_function.is_async}")
                if not fn.indexify_function.is_async:
                    fn_call_result: FunctionCallResult = fn.invoke_fn_ser(
                        fn_name, input, init_value
                    )
                else:
                    fn_call_result: FunctionCallResult = await fn.invoke_fn_ser_async(
                        fn_name, input, init_value
                    )
                print(f"serialized function output: {fn_call_result}")
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
