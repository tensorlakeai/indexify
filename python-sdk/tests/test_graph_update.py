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
        g = Graph(name="updategraph1", start_node=update)
        g = RemoteGraph.deploy(g)
        invocation_id = g.run(block_until_done=True, x=Object(x="a"))
        output = g.output(invocation_id, fn_name="update")
        self.assertEqual(output[0], Object(x="ab"))

        g = Graph(name="updategraph1", start_node=update2)
        g = RemoteGraph.deploy(g)
        g.rerun()
        time.sleep(1)
        output = g.output(invocation_id, fn_name="update2")
        self.assertEqual(output[0], Object(x="ac"))


if __name__ == "__main__":
    unittest.main()
