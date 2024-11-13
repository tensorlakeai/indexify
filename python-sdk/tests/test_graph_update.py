import time
import unittest

from pydantic import BaseModel

from indexify import RemoteGraph
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function


class Object(BaseModel):
    x: str


@indexify_function()
def update(x: Object) -> Object:
    return Object(x=x.x + "b")


@indexify_function()
def update2(x: Object) -> Object:
    return Object(x=x.x + "c")


class TestGraphUpdate(unittest.TestCase):
    def test_graph_update(self):
        g = Graph(name=self.id(), description="test graph update", start_node=update)
        g = RemoteGraph.deploy(g)
        invocation_id = g.run(block_until_done=True, x=Object(x="a"))
        output = g.output(invocation_id, fn_name="update")
        self.assertEqual(output[0], Object(x="ab"))

        # Update the graph and rerun the invocation
        g = Graph(
            name=self.id(),
            description="test graph update (2)",
            start_node=update2,
        )
        g = RemoteGraph.deploy(g)
        g.replay_invocations()
        while g.metadata().replaying:
            time.sleep(1)
            print("Replaying...")
        output = g.output(invocation_id, fn_name="update2")
        self.assertEqual(output[0], Object(x="ac"))

        # Create more invocations to trigger server queueing.
        for i in range(10):
            invocation_id = g.run(block_until_done=True, x=Object(x=f"{i}"))

        output = g.output(invocation_id, fn_name="update2")
        self.assertEqual(output[0], Object(x="9c"))

        # Update the graph and rerun the invocation
        g = Graph(name=self.id(), description="test graph update", start_node=update)
        g = RemoteGraph.deploy(g)

        g.replay_invocations()
        while g.metadata().replaying:
            time.sleep(2)
            print("Replaying...")
        output = g.output(invocation_id, fn_name="update")
        self.assertEqual(output[0], Object(x="9b"))


if __name__ == "__main__":
    unittest.main()
