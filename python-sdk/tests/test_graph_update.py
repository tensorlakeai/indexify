import time
import unittest

from pydantic import BaseModel

from indexify import create_client
from indexify.functions_sdk.graphds import GraphDS
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
        g = GraphDS(name="updategraph1", start_node=update)
        client = create_client()
        client.register_compute_graph(g)
        invocation_id = client.invoke_graph_with_object(
            g.name, block_until_done=True, x=Object(x="a")
        )
        output = client.graph_outputs(g.name, invocation_id, fn_name="update")
        self.assertEqual(output[0], Object(x="ab"))

        g = GraphDS(name="updategraph1", start_node=update2)
        client.register_compute_graph(g)
        client.rerun_graph(g.name)
        time.sleep(1)
        output = client.graph_outputs(g.name, invocation_id, fn_name="update2")
        self.assertEqual(output[0], Object(x="ac"))


if __name__ == "__main__":
    unittest.main()
