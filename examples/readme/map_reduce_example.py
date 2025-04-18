from pydantic import BaseModel
from tensorlake import tensorlake_function, Graph
from typing import List

class Total(BaseModel):
    val: int = 0

@tensorlake_function()
def generate_numbers(a: int) -> List[int]:
    return [i for i in range(a)]

@tensorlake_function()
def square(x: int) -> int:
    return x ** 2

@tensorlake_function(accumulate=Total)
def add(total: Total, new: int) -> Total:
    total.val += new
    return total

g = Graph(name="sequence_summer", start_node=generate_numbers, description="Simple Sequence Summer")
g.add_edge(generate_numbers, square)
g.add_edge(square, add)

if __name__ == "__main__":
    #invocation_id = g.run(a=10)
    #result = g.get_output(invocation_id, "add")
    #print(result)

    from tensorlake import RemoteGraph
    graph = RemoteGraph.deploy(g)
    invocation_id = graph.run(block_until_done=True, a=10)
    result = graph.output(invocation_id, "add")
    print(result)

    graph = RemoteGraph.by_name("sequence_summer")
    invocation_id = graph.run(block_until_done=True, a=5)
    print(graph.output(invocation_id, "add"))
