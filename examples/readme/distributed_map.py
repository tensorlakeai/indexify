from tensorlake import tensorlake_function, Graph
from typing import List

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

    num_iter = 90
    invocation_id = graph.run(block_until_done=True, a=num_iter)
    result = graph.output(invocation_id, "squared")
    if len(result) != num_iter:
        raise Exception(f"Missing outputs - {len(result)} != {num_iter}")
    else:
        print(f"Success with {num_iter} outputs")
