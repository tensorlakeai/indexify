import unittest

from tensorlake import (
    Graph,
    InvocationArgumentError,
    RemoteGraph,
    Retries,
    TensorlakeCompute,
    tensorlake_function,
)
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


def to_remote_graph(graph) -> RemoteGraph:
    return RemoteGraph.deploy(graph, code_dir_path=graph_code_dir_path(__file__))


@tensorlake_function()
def raise_permanent_invocation_error(x: str) -> int:
    raise InvocationArgumentError("Testing permanent invocation errors")


@tensorlake_function()
def raise_transient_invocation_error(x: str) -> int:
    raise Exception("Testing transient invocation errors")


class FunctionThatThrowsFromInit(TensorlakeCompute):
    name = "FunctionThatThrowsFromInit"

    def __init__(self):
        super().__init__()
        raise Exception("Initialization Error")

    def run(self, action: str) -> str:
        raise Exception("This function can never run because it fails to initialize.")


class TestGraphFailures(unittest.TestCase):
    def test_transient_failure(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test for failed task",
            start_node=raise_transient_invocation_error,
            retries=Retries(max_retries=3, max_delay=1.0),
        )
        graph = to_remote_graph(graph)
        invocation_id = graph.run(block_until_done=True, x=1)
        output = graph.output(invocation_id, "raise_transient_invocation_error")
        self.assertEqual(output, [])
        gm = graph.metadata()
        self.assertEqual("Ready", gm.state.status)
        im = graph.invocation_metadata(invocation_id)
        self.assertEqual("Failed", im.state.status)

    def test_permanent_invocation_failure(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test for failed invocation",
            start_node=raise_permanent_invocation_error,
            retries=Retries(max_retries=3, max_delay=1.0),
        )
        graph = to_remote_graph(graph)
        invocation_id = graph.run(block_until_done=True, x=1)
        output = graph.output(invocation_id, "raise_permanent_invocation_error")
        self.assertEqual(output, [])
        gm = graph.metadata()
        self.assertEqual("Ready", gm.state.status)
        im = graph.invocation_metadata(invocation_id)
        self.assertEqual("Failed", im.state.status)

    def test_graph_remains_ready_after_transient_failures(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test for graph readiness after failed tasks",
            start_node=raise_transient_invocation_error,
            retries=Retries(max_retries=3, max_delay=1.0),
            consecutive_failure_max=3,
        )

        graph = to_remote_graph(graph)
        invocation_id = graph.run(block_until_done=True, x=1)
        gm = graph.metadata()
        self.assertEqual(0, gm.failure_gauge.consecutive_failure_count)
        self.assertEqual("Ready", gm.state.status)
        im = graph.invocation_metadata(invocation_id)
        self.assertEqual("Failed", im.state.status)

        invocation_id = graph.run(block_until_done=True, x=1)
        gm = graph.metadata()
        self.assertEqual(0, gm.failure_gauge.consecutive_failure_count)
        self.assertEqual("Ready", gm.state.status)
        im = graph.invocation_metadata(invocation_id)
        self.assertEqual("Failed", im.state.status)

        invocation_id = graph.run(block_until_done=True, x=1)
        output = graph.output(invocation_id, "raise_transient_invocation_error")
        self.assertEqual(output, [])
        gm = graph.metadata()
        self.assertEqual(0, gm.failure_gauge.consecutive_failure_count)
        self.assertEqual("Ready", gm.state.status)
        im = graph.invocation_metadata(invocation_id)
        self.assertEqual("Failed", im.state.status)

    def test_graph_breaks_after_init_failures(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test for failed graph after init failures",
            start_node=FunctionThatThrowsFromInit,
            retries=Retries(max_retries=3, max_delay=1.0),
            consecutive_failure_max=3,
        )

        graph = to_remote_graph(graph)
        invocation_id = graph.run(block_until_done=True, x=1)
        gm = graph.metadata()
        self.assertEqual(1, gm.failure_gauge.consecutive_failure_count)
        self.assertEqual("Ready", gm.state.status)
        im = graph.invocation_metadata(invocation_id)
        self.assertEqual("Failed", im.state.status)

        invocation_id = graph.run(block_until_done=True, x=1)
        gm = graph.metadata()
        self.assertEqual(2, gm.failure_gauge.consecutive_failure_count)
        self.assertEqual("Ready", gm.state.status)
        im = graph.invocation_metadata(invocation_id)
        self.assertEqual("Failed", im.state.status)

        invocation_id = graph.run(block_until_done=True, x=1)
        output = graph.output(invocation_id, "FunctionThatThrowsFromInit")
        self.assertEqual(output, [])
        gm = graph.metadata()
        self.assertEqual(3, gm.failure_gauge.consecutive_failure_count)
        self.assertEqual("Broken", gm.state.status)
        im = graph.invocation_metadata(invocation_id)
        self.assertEqual("Failed", im.state.status)


if __name__ == "__main__":
    unittest.main()
