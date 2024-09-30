from pydantic import BaseModel
from indexify import indexify_function, indexify_router, Graph
from typing import List, Union

@indexify_function()
def generate_sequence(a: int) -> List[int]:
    return [i for i in range(a)]

class Sum(BaseModel):
    val: int = 0

@indexify_function(accumulate=Sum)
def sum_all_numbers(sum: Sum, val: int) -> Sum:
    import time
    time.sleep(1)
    val = sum.val + val
    return Sum(val=val)

@indexify_function()
def squared(sum: Sum) -> int:
    return sum.val * sum.val

@indexify_function()
def tripled(sum: Sum) -> int:
    return sum.val * sum.val * sum.val

@indexify_router()
def dynamic_router(val: Sum) -> List[Union[squared, tripled]]:
    if val.val % 2:
        return [squared]
    return [tripled]

if __name__ == '__main__':
    g = Graph(name="sequence_summer", start_node=generate_sequence, description="Simple Sequence Summer")
    g.add_edge(generate_sequence, sum_all_numbers)
    g.add_edge(sum_all_numbers, dynamic_router)
    g.route(dynamic_router, [squared, tripled])

    from indexify import create_client

    client = create_client(in_process=True)
    client.register_compute_graph(g)

    invocation_id = client.invoke_graph_with_object("sequence_summer", block_until_done=True, a=3)
    result = client.graph_outputs("sequence_summer", invocation_id, "squared")
    print(result)
    result = client.graph_outputs("sequence_summer", invocation_id, "tripled")
    print(result)
