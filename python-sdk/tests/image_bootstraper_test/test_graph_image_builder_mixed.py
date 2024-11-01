import unittest
from typing import List

from pydantic import BaseModel

from indexify import Graph, RemoteGraph, indexify_function
from indexify.functions_sdk.image import Image

"""
How to run this test
- Make sure python version in executor and cli works.
- Alias the executor,
```
docker run --network host -it indexify-python-sdk-dev indexify-cli executor \
--name-alias indexify/indexify-executor-default
```

and

```
docker run --network host -it indexify-python-sdk-dev indexify-cli executor \
--name-alias indexify/indexify-executor-default2
```

- Check the executor log for the installation.
- Test passes.
"""

image = Image().name("indexify/indexify-executor-default").run("pip install requests")

image_broken = (
    Image().name("indexify/indexify-executor-default2").run("pip install reques2223422")
)


class Total(BaseModel):
    val: int = 0


@indexify_function(image=image)
def generate_numbers(a: int) -> List[int]:
    return [i for i in range(a)]


@indexify_function(image=image_broken)
def square(x: int) -> int:
    return x**2


@indexify_function(accumulate=Total, image=image)
def add(total: Total, new: int) -> Total:
    total.val += new
    return total


class TestGraphImageBuilderWorking(unittest.TestCase):
    def test_install_working_dependency(self):
        g = Graph(
            name="sequence_summer3",
            start_node=generate_numbers,
            description="Simple Sequence Summer",
        )

        g.add_edge(generate_numbers, square)
        g.add_edge(square, add)

        RemoteGraph.deploy(g)
        graph = RemoteGraph.by_name("sequence_summer3")
        invocation_id = graph.run(block_until_done=True, a=5)
        assert len(graph.output(invocation_id, "square")) == 0


if __name__ == "__main__":
    unittest.main()
