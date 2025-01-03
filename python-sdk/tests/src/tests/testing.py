import unittest

from indexify.functions_sdk.graph import Graph
from indexify.remote_graph import RemoteGraph


def remote_or_local_graph(graph, remote=True) -> RemoteGraph | Graph:
    if remote:
        return RemoteGraph.deploy(graph)
    return graph


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
