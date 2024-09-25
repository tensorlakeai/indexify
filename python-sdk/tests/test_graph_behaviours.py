from indexify import Graph, indexify_function, indexify_router, create_client
from typing import List, Union
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

@indexify_function()
def add_two(x: Sum) -> int:
    return x.val + 2

@indexify_function()
def add_three(x: Sum) -> int:
    return x.val + 3

@indexify_router()
def route_if_even(x: Sum) -> List[Union[add_two, add_three]]:
    print(f"routing input {x}")
    if x.val % 2 == 0:
        return add_three
    else:
        return add_two


def create_pipeline_graph_with_map():
    graph = Graph(name="test", description="test", start_node=generate_seq)
    graph.add_edge(generate_seq, square)
    return graph

def create_pipeline_graph_with_map_reduce():
    graph = Graph(name="test_map_reduce", description="test", start_node=generate_seq)
    graph.add_edge(generate_seq, square)
    graph.add_edge(square, sum_of_squares)
    graph.add_edge(sum_of_squares, make_it_string)
    return graph

def create_router_graph():
    graph = Graph(name="test_router", description="test", start_node=generate_seq)
    graph.add_edge(generate_seq, square)
    graph.add_edge(square, sum_of_squares)
    graph.add_edge(sum_of_squares, route_if_even)
    graph.route(route_if_even, [add_two, add_three])
    return graph

class TestGraphBehaviors(unittest.TestCase):
    def test_graph_behavior(self):
        graph = create_pipeline_graph_with_map()
        client = create_client()
        client.register_compute_graph(graph)
        invocation_id = client.invoke_graph_with_object(graph.name, block_until_done=True, x=3)

        output_seq = client.graph_outputs(graph.name, invocation_id, "generate_seq")
        self.assertEqual(sorted(output_seq), [0, 1, 2])

        output_sq = client.graph_outputs(graph.name, invocation_id, "square")
        self.assertEqual(sorted(output_sq), [0, 1, 4])

    def test_graph_behavior_with_map_reduce(self):
        graph = create_pipeline_graph_with_map_reduce()
        client = create_client()
        client.register_compute_graph(graph)
        invocation_id = client.invoke_graph_with_object(graph.name, block_until_done=True, x=3)

        output_sum_sq = client.graph_outputs(graph.name, invocation_id, "sum_of_squares")
        self.assertEqual(output_sum_sq, [Sum(val=5)])

        output_str = client.graph_outputs(graph.name, invocation_id, "make_it_string")
        self.assertEqual(output_str, ["5"])

    def test_router_graph_behavior(self):
        graph = create_router_graph()
        client = create_client()
        client.register_compute_graph(graph)
        invocation_id = client.invoke_graph_with_object(graph.name, block_until_done=True, x=3)

        output_add_two = client.graph_outputs(graph.name, invocation_id, "add_two")
        output_add_three = client.graph_outputs(graph.name, invocation_id, "add_three")
        self.assertEqual(output_add_two, [7])
        self.assertEqual(output_add_three, [])
        

if __name__ == "__main__":
    unittest.main()
