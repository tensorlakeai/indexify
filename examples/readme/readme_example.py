from pydantic import BaseModel
from indexify import indexify_function, indexify_router
from typing import List, Union

from indexify.functions_sdk.graph import Graph


class Total(BaseModel):
    val: int = 0

@indexify_function()
def generate_numbers(a: int) -> List[int]:
    return [i for i in range(a)]

@indexify_function(accumulate=Total)
def add(total: Total, new: int) -> Total:
    total.val += new
    return total

@indexify_function()
def square(total: Total) -> int:
    return total.val ** 2

@indexify_function()
def cube(total: Total) -> int:
    return total.val ** 3

@indexify_router()
def dynamic_router(val: Total) -> List[Union[square, cube]]:
    if val.val % 2:
        return [square]
    return [cube]


def run_local(graph_name, a, fn):
    g = Graph(
        name=graph_name,
        start_node=generate_numbers,
        description="Simple Sequence Summer",
    )
    g.add_edge(generate_numbers, add)
    g.add_edge(add, dynamic_router)
    g.route(dynamic_router, [square, cube])

    invocation_id = g.run(a=a, block_until_done=True)
    result = g.graph_outputs(invocation_id, fn)
    print(result)


def run_remote(graph_name, a, fn):
    g = Graph(
        name=graph_name,
        start_node=generate_numbers,
        description="Simple Sequence Summer",
        server_url="http://localhost:8900",
    )
    g.add_edge(generate_numbers, add)
    g.add_edge(add, dynamic_router)
    g.route(dynamic_router, [square, cube])

    invocation_id = g.run(a=a, block_until_done=True)
    result = g.graph_outputs(invocation_id, fn)
    print(result)

def register_remote_graph(graph_name):
    g = Graph(
        name=graph_name,
        start_node=generate_numbers,
        description="Simple Sequence Summer",
        server_url="http://localhost:8900",
    )
    g.add_edge(generate_numbers, add)
    g.add_edge(add, dynamic_router)
    g.route(dynamic_router, [square, cube])

    g.register_remote()


if __name__ == '__main__':
    pass

    ### Test locally
    # graph_name = "sequence_summer"
    # run_local(graph_name=graph_name, a=5, fn="cube")

    ### Test remote
    # graph_name = "sequence_summer"
    # run_remote(graph_name=graph_name, a=6, fn="square")

    ### Test the graph with a remote run, make sure to register it first
    # graph_name = "sequence-summer"
    # register_remote_graph(graph_name=graph_name)

    # g = Graph.from_server(server_url="http://localhost:8900", namespace="default", name=graph_name)
    # invocation_id = g.run(a=6, block_until_done=True)
    # result = g.graph_outputs(invocation_id, "square")
    # print(result)