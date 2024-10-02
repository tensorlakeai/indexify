from pydantic import BaseModel
from indexify import indexify_function, Graph, indexify_router
from typing import List, Union

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

if __name__ == '__main__':
    g = Graph(name="sequence_summer", start_node=generate_numbers, description="Simple Sequence Summer")
    g.add_edge(generate_numbers, add)
    g.add_edge(add, dynamic_router)
    g.route(dynamic_router, [square, cube])

    from indexify import create_client

    client = create_client(in_process=True)
    client.register_compute_graph(g)

    invocation_id = client.invoke_graph_with_object("sequence_summer", block_until_done=True, a=3)
    result = client.graph_outputs("sequence_summer", invocation_id, "squared")
    print(result)
    result = client.graph_outputs("sequence_summer", invocation_id, "tripled")
    print(result)
