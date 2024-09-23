import asyncio
import concurrent
import sys
import traceback
from concurrent.futures.process import BrokenProcessPool
from typing import Dict, List, Union

from rich import print

from indexify.functions_sdk.data_objects import IndexifyData, RouterOutput
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import IndexifyFunctionWrapper

graphs: Dict[str, Graph] = {}
function_wrapper_map: Dict[str, IndexifyFunctionWrapper] = {}


def _load_function(namespace: str, graph_name: str, fn_name: str, code_path: str):
    """Load an extractor to the memory: extractor_wrapper_map."""
    global function_wrapper_map
    key = f"{namespace}/{graph_name}/{fn_name}"
    if key in function_wrapper_map:
        return
    graph = Graph.from_path(code_path)
    function_wrapper = graph.get_function(fn_name)
    function_wrapper_map[key] = function_wrapper
    graph_key = f"{namespace}/{graph_name}"
    graphs[graph_key] = graph


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
    ) -> List[IndexifyData]:
        try:
            resp = await asyncio.get_running_loop().run_in_executor(
                self._executor,
                _run_function,
                namespace,
                graph_name,
                fn_name,
                input,
                code_path,
            )
        except BrokenProcessPool as mp:
            self._executor.shutdown(wait=True, cancel_futures=True)
            raise mp
        except Exception as e:
            raise Exception(traceback.format_exc())

        return resp

    def shutdown(self):
        self._executor.shutdown(wait=True, cancel_futures=True)


def _run_function(
    namespace: str,
    graph_name: str,
    fn_name: str,
    input: IndexifyData,
    code_path: str,
) -> Union[List[IndexifyData], RouterOutput]:
    print(
        f"[bold] function worker: [/bold] running function: {fn_name} namespace: {namespace} graph: {graph_name}"
    )
    key = f"{namespace}/{graph_name}/{fn_name}"
    if key not in function_wrapper_map:
        _load_function(namespace, graph_name, fn_name, code_path)

    graph: Graph = graphs[f"{namespace}/{graph_name}"]
    if fn_name in graph.routers:
        return graph.invoke_router(fn_name, input)

    output = graph.invoke_fn_ser(fn_name, input)

    return output
