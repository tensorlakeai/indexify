import os
import unittest
from typing import List, Union

from tensorlake import (
    Graph,
    RemoteGraph,
    tensorlake_function,
    tensorlake_router,
)
from testing import test_graph_name


class TestGraphSecretsAreSettableButEmpty(unittest.TestCase):
    # OSS Executor doesn't implement secrets. Verify that in the test.

    def test_function(self):
        @tensorlake_function(secrets=["SECRET_NAME"])
        def check_function_secret_not_set() -> bool:
            return "SECRET_NAME" not in os.environ

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=check_function_secret_not_set,
        )
        graph = RemoteGraph.deploy(graph)
        invocation_id = graph.run(block_until_done=True)
        output = graph.output(invocation_id, "check_function_secret_not_set")
        self.assertTrue(len(output) == 1)
        self.assertEqual(output[0], True)

    def test_router(self):
        @tensorlake_function()
        def success_func() -> str:
            return "success"

        @tensorlake_function()
        def fail_func() -> str:
            return "failure"

        @tensorlake_router(secrets=["SECRET_NAME_ROUTER"])
        def route_if_secret_not_set() -> List[Union[success_func, fail_func]]:
            if "SECRET_NAME_ROUTER" in os.environ:
                return fail_func
            else:
                return success_func

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=route_if_secret_not_set,
        )

        graph.route(route_if_secret_not_set, [success_func, fail_func])
        graph = RemoteGraph.deploy(graph)
        invocation_id = graph.run(block_until_done=True)
        output = graph.output(invocation_id, "success_func")
        self.assertTrue(len(output) == 1)
        self.assertEqual(output[0], "success")


if __name__ == "__main__":
    unittest.main()
