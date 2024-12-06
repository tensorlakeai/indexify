from pydantic import BaseModel
from indexify import indexify_function, Graph, Image
from typing import List

class Total(BaseModel):
    val: int = 0


image_test = (
    Image().name("readme/adder")
    .run("pip install lancedb")
    .run("pip install openai")
    .run("pip install langchain")
#    .run("pip install requests")
)

@indexify_function(image=image_test)
def generate_numbers(a: int) -> List[int]:
    return [i for i in range(a)]

@indexify_function()
def square(x: int) -> int:
    return x ** 2

@indexify_function(accumulate=Total)
def add(total: Total, new: int) -> Total:
    total.val += new
    return total

g = Graph(name="sequence_summer", start_node=generate_numbers, description="Simple Sequence Summer")
g.add_edge(generate_numbers, square)
g.add_edge(square, add)

print(g.definition().model_dump_json(exclude_none=True))

if __name__ == "__main__":
    from indexify import RemoteGraph
    graph = RemoteGraph.deploy(g, server_url="http://localhost:8900")
    invocation_id = graph.run(block_until_done=True, a=100)
    result = graph.output(invocation_id, "add")
    print(result)
