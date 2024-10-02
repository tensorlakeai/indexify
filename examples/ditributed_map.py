from pydantic import BaseModel
from indexify import indexify_function, indexify_router, Graph
from typing import List, Union

@indexify_function()
def generate_sequence(a: int) -> List[int]:
    return [i for i in range(a)]

@indexify_function()
def squared(x: int) -> int:
    return x * x 

if __name__ == '__main__':
    from indexify import create_client
    g = Graph(name="map_square", start_node=generate_sequence, description="Simple Sequence Summer")
    g.add_edge(generate_sequence, squared)

    client = create_client(service_url="http://100.106.216.46:8900")
    client.register_compute_graph(g)

    invocation_id = client.invoke_graph_with_object(g.name, block_until_done=True, a=100)
    result = client.graph_outputs(g.name, invocation_id, "squared")
    print(result)
