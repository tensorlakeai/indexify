from pydantic import BaseModel
from tensorlake import tensorlake_function, tensorlake_router, Graph
from typing import List, Union

@tensorlake_function()
def generate_sequence(a: int) -> List[int]:
    return [i for i in range(a)]

@tensorlake_function()
def squared(x: int) -> int:
    return x * x

if __name__ == '__main__':
    g = Graph(name="sequence_summer", start_node=generate_sequence, description="Simple Sequence Summer")
    g.add_edge(generate_sequence, squared)

    from tensorlake import RemoteGraph
    graph = RemoteGraph.deploy(g)

    invocation_id = graph.run(block_until_done=True, a=90)
    result = graph.output(invocation_id, "squared")
    print(result)
