import unittest
from typing import Union

from tensorlake import Graph, RemoteGraph
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path


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


def remote_or_local_graph(graph, remote=True) -> Union[RemoteGraph, Graph]:
    if remote:
        return RemoteGraph.deploy(graph, code_dir_path=graph_code_dir_path(__file__))
    return graph
