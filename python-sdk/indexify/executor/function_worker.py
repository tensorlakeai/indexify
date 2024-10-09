import asyncio
import traceback
from concurrent.futures.process import BrokenProcessPool
from typing import Dict, List, Optional

import cloudpickle
from pydantic import BaseModel
from rich import print

from indexify.functions_sdk.data_objects import (
    FunctionWorkerOutput,
    IndexifyData,
    RouterOutput,
)
from indexify.functions_sdk.indexify_functions import IndexifyFunctionWrapper

function_wrapper_map: Dict[str, IndexifyFunctionWrapper] = {}

import concurrent.futures


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
    exception: Optional[str] = None
    stdout: str = ""
    stderr: str = ""


def _load_function(
    namespace: str, graph_name: str, fn_name: str, code_path: str, version: int
):
    """Load an extractor to the memory: extractor_wrapper_map."""
    global function_wrapper_map
    key = f"{namespace}/{graph_name}/{version}/{fn_name}"
    if key in function_wrapper_map:
        return
    with open(code_path, "rb") as f:
        code = f.read()
        pickled_functions = cloudpickle.loads(code)
    function_wrapper = IndexifyFunctionWrapper(
        cloudpickle.loads(pickled_functions[fn_name])
    )
    function_wrapper_map[key] = function_wrapper


class FunctionWorker:
    def __init__(self, workers: int = 1) -> None:
        self._executor: concurrent.futures.ProcessPoolExecutor = (
            concurrent.futures.ProcessPoolExecutor(max_workers=workers)
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
    ) -> FunctionWorkerOutput:
        try:
            result = await asyncio.get_running_loop().run_in_executor(
                self._executor,
                _run_function,
                namespace,
                graph_name,
                fn_name,
                input,
                code_path,
                version,
                init_value,
            )
        except BrokenProcessPool as mp:
            self._executor.shutdown(wait=True, cancel_futures=True)
            traceback.print_exc()
            raise mp
        except FunctionRunException as e:
            return FunctionWorkerOutput(
                exception=str(e),
                stdout=e.stdout,
                stderr=e.stderr,
                reducer=e.is_reducer,
                success=False,
            )

        return FunctionWorkerOutput(
            fn_outputs=result.fn_outputs,
            router_output=result.router_output,
            exception=result.exception,
            stdout=result.stdout,
            stderr=result.stderr,
            reducer=result.reducer,
            success=result.success,
        )

    def shutdown(self):
        self._executor.shutdown(wait=True, cancel_futures=True)


def _run_function(
    namespace: str,
    graph_name: str,
    fn_name: str,
    input: IndexifyData,
    code_path: str,
    version: int,
    init_value: Optional[IndexifyData] = None,
) -> FunctionOutput:
    import io
    import traceback
    from contextlib import redirect_stderr, redirect_stdout

    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    is_reducer = False
    router_output = None
    fn_output = None
    has_failed = False
    exception_msg = None
    print(
        f"[bold] function_worker: [/bold] invoking function {fn_name} in graph {graph_name}"
    )
    with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
        try:
            key = f"{namespace}/{graph_name}/{version}/{fn_name}"
            if key not in function_wrapper_map:
                _load_function(namespace, graph_name, fn_name, code_path, version)

            fn = function_wrapper_map[key]
            if (
                str(type(fn.indexify_function))
                == "<class 'indexify.functions_sdk.indexify_functions.IndexifyRo'>"
            ):
                router_output = fn.invoke_router(fn_name, input)
            else:
                fn_output = fn.invoke_fn_ser(fn_name, input, init_value)

                is_reducer = fn.indexify_function.accumulate is not None
        except Exception as e:
            import sys

            print(traceback.format_exc(), file=sys.stderr)
            has_failed = True
            exception_msg = str(e)

    # WARNING - IF THIS FAILS, WE WILL NOT BE ABLE TO RECOVER
    # ANY LOGS
    if has_failed:
        return FunctionOutput(
            fn_outputs=None,
            router_output=None,
            exception=exception_msg,
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
