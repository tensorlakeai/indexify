import os
import subprocess
import time
import unittest
from typing import Any, List, Optional

from tensorlake import RemoteGraph
from tensorlake.error import GraphStillProcessing


def test_graph_name(test_case: unittest.TestCase) -> str:
    """Converts a test case to a unique graph name.

    Example:
    >>> class TestGraphReduce(unittest.TestCase):
    ...     def test_simple(self):
    ...         g = Graph(name=graph_name(self), start_node=generate_seq)
    ...         # ...
    ...         print(g.name)
    ...         # test_graph_reduce_test_simple
    """
    return unittest.TestCase.id(test_case).replace(".", "_")


def function_uri(
    namespace: str, graph: str, function: str, version: Optional[str] = None
) -> str:
    if version is None:
        return ":".join([namespace, graph, function])
    else:
        return ":".join([namespace, graph, function, version])


class ExecutorProcessContextManager:
    def __init__(
        self,
        args: List[str],
        keep_std_outputs: bool = True,
        extra_env: Optional[dict] = None,
    ):
        self._args = ["indexify-cli", "executor"]
        self._args.extend(args)
        self._keep_std_outputs = keep_std_outputs
        self._extra_env = extra_env
        self._process: Optional[subprocess.Popen] = None

    def __enter__(self) -> subprocess.Popen:
        kwargs = {}
        if not self._keep_std_outputs:
            kwargs["stdout"] = subprocess.DEVNULL
            kwargs["stderr"] = subprocess.DEVNULL
        if self._extra_env is not None:
            kwargs["env"] = os.environ.copy()
            kwargs["env"].update(self._extra_env)
        self._process = subprocess.Popen(self._args, **kwargs)
        return self._process

    def __exit__(self, exc_type, exc_value, traceback):
        if self._process:
            self._process.terminate()
            self._process.wait()


def wait_executor_startup(port: int):
    import time

    import httpx

    print(f"Waiting 5 secs for Executor to start at port {port}")
    attempts_left: int = 5
    while attempts_left > 0:
        try:
            response = httpx.get(f"http://localhost:{port}/monitoring/startup")
            if response.status_code == 200:
                print(f"Executor startup check successful at port {port}")
                return
        except Exception:
            if attempts_left == 1:
                raise

        attempts_left -= 1
        time.sleep(1)


def executor_pid() -> int:
    # Assuming Subprocess Function Executors are used in Open Source.
    return os.getppid()


def function_executor_id() -> str:
    # PIDs are good for Subprocess Function Executors.
    return os.getpid()


def wait_function_output(graph: RemoteGraph, invocation_id: str, func_name: str) -> Any:
    while True:
        try:
            return graph.output(invocation_id, func_name)
        except GraphStillProcessing:
            time.sleep(1)
