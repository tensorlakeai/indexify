import os
import shutil
import subprocess
import tempfile
import time
import unittest
from typing import List

import httpx


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
    namespace: str, graph: str, function: str, version: str | None = None
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
        extra_env: dict | None = None,
        labels: dict | None = None,
    ):
        self._keep_std_outputs = keep_std_outputs
        self._extra_env = extra_env
        self._labels = labels or {}
        self._temp_dir = tempfile.mkdtemp(prefix="executor_cache_")
        self._process: subprocess.Popen | None = None
        self._args = [
            "indexify-cli",
            "executor",
            "--executor-cache-path",
            self._temp_dir,
        ]

        # Add label arguments in the format --label key=value
        for key, value in self._labels.items():
            self._args.extend(["--label", f"{key}={value}"])

        self._args.extend(args)

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
        if self._temp_dir:
            try:
                shutil.rmtree(self._temp_dir)
            except Exception as e:
                print(
                    f"Warning: Failed to clean up temp directory {self._temp_dir}: {e}"
                )


def wait_executor_startup(port: int):
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
        print(
            f"Waiting Executor to start at port {port} (attempts left: {attempts_left})"
        )
        time.sleep(1)


def executor_pid() -> int:
    # Assuming Subprocess Function Executors are used in Open Source.
    return os.getppid()


def function_executor_id() -> str:
    # PIDs are good for Subprocess Function Executors.
    return os.getpid()
