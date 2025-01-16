import subprocess
import unittest
from typing import List, Optional


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


def function_uri(namespace: str, graph: str, function: str, version: str) -> str:
    return ":".join([namespace, graph, function, version])


class ExecutorProcessContextManager:
    def __init__(self, args: List[str]):
        self._args = ["indexify-cli", "executor"]
        self._args.extend(args)
        self._process: Optional[subprocess.Popen] = None

    def __enter__(self) -> subprocess.Popen:
        self._process = subprocess.Popen(self._args)
        return self._process

    def __exit__(self, exc_type, exc_value, traceback):
        if self._process:
            self._process.terminate()
            self._process.wait()
