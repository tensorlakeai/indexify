from indexify import Graph, indexify_function, create_client
from typing import List
from pydantic import BaseModel
import unittest

@indexify_function()
def generate_seq(x: int) -> List[int]:
    return list(range(x))


@indexify_function()
def square(x: int) -> int:
    return x * x

class Sum(BaseModel):
    val: int = 0

@indexify_function(accumulate=Sum)
def sum_of_squares(init_value: Sum, x: int) -> Sum:
    init_value.val += x
    return init_value

@indexify_function()
def make_it_string(x: Sum) -> str:
    return str(x.val)


def create_pipeline_graph_with_map():
    graph = Graph(name="test", description="test", start_node=generate_seq)
    graph.add_edge(generate_seq, square)
    return graph

def create_pipeline_graph_with_map_reduce():
    graph = Graph(name="test_map_reduce", description="test", start_node=generate_seq)
    graph.add_edge(generate_seq, square)
    graph.add_edge(square, sum_of_squares)
    #graph.add_edge(sum_of_squares, make_it_string)
    return graph

class TestGraphBehaviours(unittest.TestCase):
    # def test_graph_behavior(self):
    #     graph = create_pipeline_graph_with_map()
    #     client = create_client()
    #     client.register_compute_graph(graph)
    #     invocation_id = client.invoke_graph_with_object(graph.name, block_until_done=True, x=3)

    #     output_seq = client.graph_outputs(graph.name, invocation_id, "generate_seq")
    #     self.assertEqual(sorted(output_seq), [0, 1, 2])

    #     output_sq = client.graph_outputs(graph.name, invocation_id, "square")
    #     self.assertEqual(sorted(output_sq), [0, 1, 4])

    def test_graph_behavior_with_map_reduce(self):
        graph = create_pipeline_graph_with_map_reduce()
        client = create_client()
        client.register_compute_graph(graph)
        invocation_id = client.invoke_graph_with_object(graph.name, block_until_done=True, x=3)

        output_sum_sq = client.graph_outputs(graph.name, invocation_id, "sum_of_squares")
        self.assertEqual(output_sum_sq, [Sum(val=5)])

        output_str = client.graph_outputs(graph.name, invocation_id, "make_it_string")
        self.assertEqual(output_str, ["5"])

if __name__ == "__main__":
    unittest.main()
