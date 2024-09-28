from pydantic import BaseModel
from indexify import indexify_function, Graph
from typing import List

@indexify_function()
def generate_sequence(a: int) -> List[int]:
    return [i for i in range(a)]

class Sum(BaseModel):
    val: int = 0

@indexify_function(accumulate=Sum)
def sum_all_numbers(sum: Sum, val: int) -> Sum:
    val += sum.val
    return Sum(val=val)

@indexify_function()
def square_of_sum(sum: Sum) -> int:
    return sum.val * sum.val


def create_graph() -> Graph:
    g = Graph(name="sequence_summer", start_node=generate_sequence, description="Simple Sequence Summer")
    g.add_edge(generate_sequence, sum_all_numbers)
    g.add_edge(sum_all_numbers, square_of_sum)
    return g


if __name__ == "__main__":
    from indexify import create_client 
    g = create_graph()
    client = create_client(local=True)
    client.register_compute_graph(g)

    invocation_id = client.invoke_graph_with_object("sequence_summer", a=10)
    result = client.graph_outputs("sequence_summer", invocation_id,  "square_of_sum")
    print(result)