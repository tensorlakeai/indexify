import time
import unittest

from parameterized import parameterized
from pydantic import BaseModel

from indexify import RemoteGraph
from indexify.error import GraphStillProcessing
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function
from tests.testing import test_graph_name


class TestGraphUpdate(unittest.TestCase):
    def test_graph_update_and_replay(self):
        graph_name = test_graph_name(self)

        class Object(BaseModel):
            x: str

        @indexify_function()
        def update(x: Object) -> Object:
            return Object(x=x.x + "b")

        @indexify_function()
        def update2(x: Object) -> Object:
            return Object(x=x.x + "c")

        g = Graph(
            name=graph_name,
            description="test graph update",
            start_node=update,
        )
        g = RemoteGraph.deploy(g)
        invocation_id = g.run(block_until_done=True, x=Object(x="a"))
        output = g.output(invocation_id, fn_name="update")
        self.assertEqual(output[0], Object(x="ab"))

        # Update the graph and rerun the invocation
        g = Graph(
            name=graph_name,
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
        g = Graph(name=graph_name, description="test graph update", start_node=update)
        g = RemoteGraph.deploy(g)

        g.replay_invocations()
        while g.metadata().replaying:
            time.sleep(2)
            print("Replaying...")
        output = g.output(invocation_id, fn_name="update")
        self.assertEqual(output[0], Object(x="9b"))

    @parameterized.expand(
        ["second_graph_new_name", "second_graph_reused_function_names"]
    )
    def test_running_invocation_unaffected_by_update(self, second_graph_name: str):
        graph_name = test_graph_name(self)

        def initial_graph():
            @indexify_function()
            def start_node(x: int) -> int:
                # Sleep to provide enough time for a graph update to happen
                # while this graph version is running.
                time.sleep(1)
                return x

            @indexify_function()
            def middle_node(x: int) -> int:
                return x + 1

            @indexify_function()
            def end_node(x: int) -> int:
                return x + 2

            g = Graph(name=graph_name, start_node=start_node)
            g.add_edge(start_node, middle_node)
            g.add_edge(middle_node, end_node)
            return g

        def second_graph_new_name():
            @indexify_function()
            def start_node2(x: int) -> dict:
                return {"data": dict(num=x)}

            @indexify_function()
            def middle_node2(data: dict) -> dict:
                return {"data": dict(num=data["num"] + 1)}

            @indexify_function()
            def end_node2(data: dict) -> int:
                return data["num"] + 3

            g = Graph(name=graph_name, start_node=start_node2)
            g.add_edge(start_node2, middle_node2)
            g.add_edge(middle_node2, end_node2)
            return g, end_node2.name

        def second_graph_reused_function_names():
            @indexify_function()
            def start_node(x: int) -> dict:
                return {"data" :dict(num=x)}

            @indexify_function()
            def middle_node(data: dict) -> dict:
                return {"data": dict(num=data["num"] + 1)}

            @indexify_function()
            def end_node(data: dict) -> int:
                return data["num"] + 3

            g = Graph(name=graph_name, start_node=start_node)
            g.add_edge(start_node, middle_node)
            g.add_edge(middle_node, end_node)
            return g, end_node.name

        g = initial_graph()
        g = RemoteGraph.deploy(g)
        first_invocation_id = g.run(block_until_done=False, x=0)

        if second_graph_name == "second_graph_new_name":
            g, end_node_name = second_graph_new_name()
        else:
            g, end_node_name = second_graph_reused_function_names()
        # The first invocation should not be affected by the second graph version
        # This loop waits for the first invocation to finish and checks its output.
        time.sleep(0.25)
        g = RemoteGraph.deploy(g)
        g.metadata()
        invocation_id = g.run(block_until_done=True, x=0)
        output = g.output(invocation_id, fn_name=end_node_name)
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0], 4)

        # The first invocation should not be affected by the second graph version
        # This loop waits for the first invocation to finish and checks its output.
        while True:
            try:
                output = g.output(first_invocation_id, fn_name="end_node")
                self.assertEqual(len(output), 1, output)
                self.assertEqual(output[0], 3)
                break
            except GraphStillProcessing:
                time.sleep(1)


if __name__ == "__main__":
    unittest.main()
