from indexify.functions_sdk.image import Image
from pydantic import BaseModel
from indexify import indexify_function, Graph
from typing import List


"""
How to run this test
- Make sure python version in executor and cli works.
- Alias the executor,
```
docker run --network host -it indexify-python-sdk-dev indexify-cli executor \
--name-alias indexify/indexify-executor-default
```

- Check the executor log for the installation.
- Check that the output is val[30].
"""

image = Image().name("indexify/indexify-executor-default")


class Total(BaseModel):
    val: int = 0


@indexify_function(image=image)
def generate_numbers(a: int) -> List[int]:
    return [i for i in range(a)]


@indexify_function(image=image)
def square(x: int) -> int:
    return x**2


@indexify_function(accumulate=Total, image=image)
def add(total: Total, new: int) -> Total:
    total.val += new
    return total


g = Graph(
    name="sequence_summer",
    start_node=generate_numbers,
    description="Simple Sequence Summer",
)
g.add_edge(generate_numbers, square)
g.add_edge(square, add)

if __name__ == "__main__":
    # invocation_id = g.run(a=10)
    # result = g.get_output(invocation_id, "add")
    # print(result)

    from indexify import RemoteGraph

    graph = RemoteGraph.deploy(g)
    invocation_id = graph.run(block_until_done=True, a=10)
    result = graph.output(invocation_id, "add")
    print(result)

    graph = RemoteGraph.by_name("sequence_summer")
    invocation_id = graph.run(block_until_done=True, a=5)
    assert graph.output(invocation_id, "add")[0].val == 30
