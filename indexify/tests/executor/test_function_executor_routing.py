import unittest

from tensorlake import Graph, RemoteGraph, tensorlake_function
from testing import test_graph_name


def function_executor_id() -> str:
    # We can't use PIDs as they are reused when a process exits.
    # Use memory address of the function instead.
    return str(id(function_executor_id))


@tensorlake_function()
def get_function_executor_id_1() -> str:
    return function_executor_id()


@tensorlake_function()
def get_function_executor_id_2(id_from_1: str) -> str:
    return function_executor_id()


class TestFunctionExecutorRouting(unittest.TestCase):
    def test_functions_of_same_version_run_in_same_function_executor(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=get_function_executor_id_1,
        )
        graph = RemoteGraph.deploy(graph)

        invocation_id = graph.run(block_until_done=True)
        output = graph.output(invocation_id, "get_function_executor_id_1")
        self.assertEqual(len(output), 1)
        function_executor_id_1 = output[0]

        invocation_id = graph.run(block_until_done=True)
        output = graph.output(invocation_id, "get_function_executor_id_1")
        self.assertEqual(len(output), 1)
        function_executor_id_2 = output[0]

        self.assertEqual(function_executor_id_1, function_executor_id_2)

    def test_functions_of_different_versions_run_in_different_function_executors(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=get_function_executor_id_1,
            version="1.0",
        )
        graph1 = RemoteGraph.deploy(graph)

        invocation_id = graph1.run(block_until_done=True)
        output = graph1.output(invocation_id, "get_function_executor_id_1")
        self.assertEqual(len(output), 1)
        function_executor_id_1 = output[0]

        graph.version = "2.0"
        graph2 = RemoteGraph.deploy(graph)
        invocation_id = graph2.run(block_until_done=True)
        output = graph2.output(invocation_id, "get_function_executor_id_1")
        self.assertEqual(len(output), 1)
        function_executor_id_2 = output[0]

        self.assertNotEqual(function_executor_id_1, function_executor_id_2)

    def test_different_functions_of_same_graph_run_in_different_function_executors(
        self,
    ):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=get_function_executor_id_1,
        )
        graph.add_edge(get_function_executor_id_1, get_function_executor_id_2)
        graph = RemoteGraph.deploy(graph)

        invocation_id = graph.run(block_until_done=True)
        output = graph.output(invocation_id, "get_function_executor_id_1")
        self.assertEqual(len(output), 1)
        function_executor_id_1 = output[0]

        output = graph.output(invocation_id, "get_function_executor_id_2")
        self.assertEqual(len(output), 1)
        function_executor_id_2 = output[0]

        self.assertNotEqual(function_executor_id_1, function_executor_id_2)

    def test_same_functions_of_different_graphs_run_in_different_function_executors(
        self,
    ):
        graph1 = Graph(
            name=test_graph_name(self) + "_1",
            description="test",
            start_node=get_function_executor_id_1,
        )
        graph1 = RemoteGraph.deploy(graph1)

        graph2 = Graph(
            name=test_graph_name(self) + "_2",
            description="test",
            start_node=get_function_executor_id_1,
        )
        graph2 = RemoteGraph.deploy(graph2)

        invocation_id = graph1.run(block_until_done=True)
        output = graph1.output(invocation_id, "get_function_executor_id_1")
        self.assertEqual(len(output), 1)
        function_executor_id_1 = output[0]

        invocation_id = graph2.run(block_until_done=True)
        output = graph2.output(invocation_id, "get_function_executor_id_1")
        self.assertEqual(len(output), 1)
        function_executor_id_2 = output[0]

        self.assertNotEqual(function_executor_id_1, function_executor_id_2)


if __name__ == "__main__":
    unittest.main()
