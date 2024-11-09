import unittest
from pathlib import Path
from typing import List, Union

from pydantic import BaseModel

from indexify import (
    Graph,
    IndexifyFunction,
    Pipeline,
    RemoteGraph,
    RemotePipeline,
    get_ctx,
    indexify_function,
    indexify_router,
)
from indexify.functions_sdk.data_objects import File


class MyObject(BaseModel):
    x: str


@indexify_function()
def simple_function(x: MyObject) -> MyObject:
    return MyObject(x=x.x + "b")


@indexify_function(encoder="json")
def simple_function_with_json_encoder(x: MyObject) -> MyObject:
    return MyObject(x=x.x + "b")


@indexify_function(encoder="invalid")
def simple_function_with_invalid_encoder(x: MyObject) -> MyObject:
    return MyObject(x=x.x + "b")


class ComplexObject(BaseModel):
    invocation_id: str
    graph_name: str
    graph_version: str


@indexify_function()
def simple_function_ctx(x: MyObject) -> ComplexObject:
    ctx = get_ctx()
    ctx.set_state_key("my_key", 10)
    return ComplexObject(
        invocation_id=ctx.invocation_id,
        graph_name=ctx.graph_name,
        graph_version=ctx.graph_version,
    )


@indexify_function()
def simple_function_ctx_b(x: ComplexObject) -> int:
    ctx = get_ctx()
    val = ctx.get_state_key("my_key")
    return val + 1


class SimpleFunctionCtxC(IndexifyFunction):
    name = "SimpleFunctionCtxC"

    def __init__(self):
        super().__init__()

    def run(self, x: ComplexObject) -> int:
        ctx = get_ctx()
        print(f"ctx: {ctx}")
        val = ctx.get_state_key("my_key")
        assert val == 10
        not_present = ctx.get_state_key("not_present")
        assert not_present is None
        return val + 1


@indexify_function()
def generate_seq(x: int) -> List[int]:
    return list(range(x))


@indexify_function()
def square(x: int) -> int:
    return x * x


@indexify_function(encoder="json")
def square_with_json_encoder(x: int) -> int:
    return x * x


class Sum(BaseModel):
    val: int = 0


@indexify_function(accumulate=Sum)
def sum_of_squares(init_value: Sum, x: int) -> Sum:
    init_value.val += x
    return init_value


@indexify_function(accumulate=Sum, encoder="json")
def sum_of_squares_with_json_encoding(init_value: Sum, x: int) -> Sum:
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


@indexify_function()
def make_it_string_from_int(x: int) -> str:
    return str(x)


@indexify_function()
def handle_file(f: File) -> int:
    return len(f.data)


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


def create_pipeline_graph_with_map_reduce_with_json_encoder():
    graph = Graph(
        name="test_map_reduce", description="test", start_node=square_with_json_encoder
    )
    graph.add_edge(square_with_json_encoder, sum_of_squares_with_json_encoding)
    return graph


def create_router_graph():
    graph = Graph(name="test_router", description="test", start_node=generate_seq)
    graph.add_edge(generate_seq, square)
    graph.add_edge(square, sum_of_squares)
    graph.add_edge(sum_of_squares, route_if_even)
    graph.route(route_if_even, [add_two, add_three])
    graph.add_edge(add_two, make_it_string_from_int)
    graph.add_edge(add_three, make_it_string_from_int)
    return graph


def create_simple_pipeline():
    p = Pipeline("simple_pipeline", "A simple pipeline")
    p.add_step(generate_seq)
    p.add_step(square)
    p.add_step(sum_of_squares)
    p.add_step(make_it_string)
    return p


class TestGraphBehaviors(unittest.TestCase):
    def test_simple_function(self):
        graph = Graph(
            name="test_simple_function", description="test", start_node=simple_function
        )
        graph = RemoteGraph.deploy(graph)
        invocation_id = graph.run(block_until_done=True, x=MyObject(x="a"))
        output = graph.output(invocation_id, "simple_function")
        self.assertEqual(output, [MyObject(x="ab")])

    def test_simple_function_with_json_encoding(self):
        graph = Graph(
            name="test_simple_function_with_json_encoding", description="test", start_node=simple_function_with_json_encoder
        )
        graph = RemoteGraph.deploy(graph)
        invocation_id = graph.run(block_until_done=True, x=MyObject(x="a"))
        output = graph.output(invocation_id,
                              "simple_function_with_json_encoder")
        self.assertEqual(output, [MyObject(x='ab')])

    def test_simple_function_with_invalid_encoding(self):
        graph = Graph(
            name="test_simple_function_with_invalid_encoding", description="test", start_node=simple_function_with_invalid_encoder
        )
        graph = RemoteGraph.deploy(graph)
        invocation_id = graph.run(block_until_done=True, x=MyObject(x="a"))
        output = graph.output(invocation_id, "simple_function_with_invalid_encoder")
        self.assertEqual(output, [])

    def test_map_operation(self):
        graph = create_pipeline_graph_with_map()
        graph = RemoteGraph.deploy(graph)
        invocation_id = graph.run(block_until_done=True, x=3)
        output_seq = graph.output(invocation_id, "generate_seq")
        self.assertEqual(sorted(output_seq), [0, 1, 2])
        output_sq = graph.output(invocation_id, "square")
        self.assertEqual(sorted(output_sq), [0, 1, 4])

    def test_map_reduce_operation(self):
        graph = create_pipeline_graph_with_map_reduce()
        graph = RemoteGraph.deploy(graph)
        invocation_id = graph.run(block_until_done=True, x=3)
        output_sum_sq = graph.output(invocation_id, "sum_of_squares")
        self.assertEqual(output_sum_sq, [Sum(val=5)])
        output_str = graph.output(invocation_id, "make_it_string")
        self.assertEqual(output_str, ["5"])

    def test_map_reduce_operation_with_json_encoding(self):
        graph = create_pipeline_graph_with_map_reduce_with_json_encoder()
        graph = RemoteGraph.deploy(graph)
        invocation_id = graph.run(block_until_done=True, x=3)
        output_square_sq_with_json_encoding = graph.output(invocation_id, "square_with_json_encoder")
        self.assertEqual(output_square_sq_with_json_encoding, [9])
        output_sum_sq_with_json_encoding = graph.output(invocation_id, "sum_of_squares_with_json_encoding")
        self.assertEqual(output_sum_sq_with_json_encoding, [Sum(val=9)])

    def test_router_graph_behavior(self):
        graph = create_router_graph()
        graph = RemoteGraph.deploy(graph)
        invocation_id = graph.run(block_until_done=True, x=3)

        output_add_two = graph.output(invocation_id, "add_two")
        self.assertEqual(output_add_two, [7])
        try:
            graph.output(invocation_id, "add_three")
        except Exception as e:
            self.assertEqual(
                str(e), "no results found for fn add_three on graph test_router"
            )

        output_str = graph.output(invocation_id, "make_it_string_from_int")
        self.assertEqual(output_str, ["7"])

    def test_invoke_file(self):
        graph = Graph(
            name="test_handle_file", description="test", start_node=handle_file
        )
        graph = RemoteGraph.deploy(graph)
        import os

        data = Path(os.path.dirname(__file__) + "/test_file").read_text()
        file = File(data=data, metadata={"some_val": 2})

        invocation_id = graph.run(
            block_until_done=True,
            f=file,
        )

        output = graph.output(invocation_id, "handle_file")
        self.assertEqual(output, [11])

    def test_pipeline(self):
        p = create_simple_pipeline()
        p = RemotePipeline.deploy(p)
        p.run(x=3)
        invocation_id = p.run(block_until_done=True, x=3)
        output = p.output(invocation_id, "make_it_string")
        self.assertEqual(output, ["5"])

    def test_ignore_none_in_map(self):
        @indexify_function()
        def gen_seq(x: int) -> List[int]:
            return list(range(x))

        @indexify_function()
        def ignore_none(x: int) -> int:
            if x % 2 == 0:
                return x
            return None

        @indexify_function()
        def add_two(x: int) -> int:
            return x + 2

        graph = Graph(name="test_ignore_none", description="test", start_node=gen_seq)
        graph.add_edge(gen_seq, ignore_none)
        graph.add_edge(ignore_none, add_two)
        graph = RemoteGraph.deploy(graph)
        invocation_id = graph.run(block_until_done=True, x=5)
        output = graph.output(invocation_id, "add_two")
        self.assertEqual(sorted(output), [2, 4, 6])

    def test_graph_context(self):
        graph = Graph(
            name="test_context", description="test", start_node=simple_function_ctx
        )
        graph.add_edge(simple_function_ctx, simple_function_ctx_b)
        graph = RemoteGraph.deploy(graph)
        invocation_id = graph.run(block_until_done=True, x=MyObject(x="a"))
        output2 = graph.output(invocation_id, "simple_function_ctx_b")
        self.assertEqual(output2[0], 11)
        graph1 = Graph(
            name="test_context", description="test", start_node=simple_function_ctx
        )
        graph1.add_edge(simple_function_ctx, SimpleFunctionCtxC)
        graph1 = RemoteGraph.deploy(graph1)
        invocation_id = graph1.run(block_until_done=True, x=MyObject(x="a"))
        output2 = graph1.output(invocation_id, "SimpleFunctionCtxC")
        self.assertEqual(output2[0], 11)

    def test_graph_router_start_node(self):
        graph = Graph(name="test_router", description="test", start_node=route_if_even)
        graph.route(route_if_even, [add_two, add_three])
        graph = RemoteGraph.deploy(graph)
        invocation_id = graph.run(block_until_done=True, x=Sum(val=2))
        output = graph.output(invocation_id, "add_three")
        self.assertEqual(output, [5])


if __name__ == "__main__":
    unittest.main()
