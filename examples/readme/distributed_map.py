from pydantic import BaseModel
from indexify import indexify_function, indexify_router, GraphDS
from typing import List, Union

@indexify_function()
def generate_sequence(a: int) -> List[int]:
    return [i for i in range(a)]

@indexify_function()
def squared(x: int) -> int:
    return x * x

if __name__ == '__main__':
    g = GraphDS(name="sequence_summer", start_node=generate_sequence, description="Simple Sequence Summer")
    g.add_edge(generate_sequence, squared)

    from indexify import create_client

    client = create_client()
    client.register_compute_graph(g)

    invocation_id = client.invoke_graph_with_object("sequence_summer",
            block_until_done=True, a=90)
    result = client.graph_outputs("sequence_summer", invocation_id, "squared")
    print(result)
