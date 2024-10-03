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
    g = Graph(name="sequence_summer", start_node=generate_sequence, description="Simple Sequence Summer")
    g.add_edge(generate_sequence, squared)

    from indexify import RemoteGraph
    graph = RemoteGraph.deploy(g)

    invocation_id = g.run(block_until_done=True, a=90)
    result = g.get_output(invocation_id, "squared")
    print(result)
