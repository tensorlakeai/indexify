import unittest
from typing import List

from pydantic import BaseModel

from indexify import Graph, RemoteGraph, indexify_function
from indexify.functions_sdk.image import Image

"""
## What is this test?
- This test is to verify that updating an existing graph (ie. version bump)
doesn't break functionality. We want to test if the executor placement
constraint is being honoured.

## How to run this test
- Make sure python version in executor and cli works.
- Alias the executors,
```
docker run --network host -it indexify-python-sdk-dev indexify-cli executor \
--name-alias tensorlake/indexify-executor-default --image-version 1
```

- Remove the local server cache (or name the graph to something else)

- Check the executor log for the installation.
- Check that the output is val[30].
- Verify that the

- Modify the run string for the method (see commented out images below)
- Alias the executor with a version +1
- Return the test to pass like above.
- Repeat
"""

# Use default image
# image1 = Image().name("tensorlake/indexify-executor-default")
# image1 = Image().name("tensorlake/indexify-executor-default").run("ls /")
image1 = Image().name("tensorlake/indexify-executor-default").run("ls /one")


class Total(BaseModel):
    val: int = 0


@indexify_function(image=image1)
def generate_numbers(a: int) -> List[int]:
    return [i for i in range(a)]


@indexify_function(image=image1)
def square(x: int) -> int:
    return x**2


@indexify_function(accumulate=Total, image=image1)
def add(total: Total, new: int) -> Total:
    total.val += new
    return total


class TestGraphImageBuilderWorking(unittest.TestCase):
    def test_install_working_dependency(self):
        g = Graph(
            name="sequence_summer7",
            start_node=generate_numbers,
            description="Simple Sequence Summer",
        )

        g.add_edge(generate_numbers, square)
        g.add_edge(square, add)

        RemoteGraph.deploy(g)
        graph = RemoteGraph.by_name("sequence_summer7")
        invocation_id = graph.run(block_until_done=True, a=5)
        assert graph.output(invocation_id, "add")[0].val == 30


if __name__ == "__main__":
    unittest.main()
