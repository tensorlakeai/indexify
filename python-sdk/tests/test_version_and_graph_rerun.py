import unittest
from typing import List

from indexify import Graph, indexify_function
from indexify.client import create_client


@indexify_function()
def generate_seq(x: int) -> List[int]:
    return list(range(x))


@indexify_function()
def square(x: int) -> int:
    return x * x


@indexify_function()
def add_one(x: int) -> int:
    return x + 1


def create_graph() -> Graph:
    graph = Graph(name="test", description="test", start_node=generate_seq)
    graph.add_edge(generate_seq, square)
    return graph


def create_graph_v2() -> Graph:
    graph = Graph(name="test", description="test", start_node=generate_seq)
    graph.add_edge(generate_seq, add_one)
    return graph


class TestVersionAndGraphRerun(unittest.TestCase):
    def test_version_and_graph_rerun(self):
        graph = create_graph()
        client = create_client()
        client.register_compute_graph(graph)
        invocation_id = client.invoke_graph_with_object(
            graph.name, block_until_done=True, x=3
        )
        print(
            client.graph_outputs(
                graph.name, invocation_id=invocation_id, fn_name="square"
            )
        )

        # client.register_compute_graph(create_graph_v2())
        # invocation_id = client.invoke_graph_with_object(graph.name, block_until_done=True, x=3)
        # print(client.graph_outputs(graph.name, invocation_id=invocation_id, fn_name="add_one"))


if __name__ == "__main__":
    unittest.main()
