import time
import unittest

from tensorlake import (
    Graph,
    RemoteGraph,
    TensorlakeCompute,
    tensorlake_function,
)
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
from testing import test_graph_name


class FunctionThatSleepsForeverOnInitialization(TensorlakeCompute):
    name = "FunctionThatSleepsForeverOnInitialization"
    timeout = 2

    def __init__(self):
        super().__init__()
        time.sleep(1000000)

    def run(self, action: str) -> str:
        raise Exception("This function can never run because it fails to initialize.")


@tensorlake_function(timeout=3)
def function_that_sleeps_forever_when_running() -> str:
    time.sleep(1000000)
    return "success"


class TestFunctionTimeouts(unittest.TestCase):
    def test_initilization(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=FunctionThatSleepsForeverOnInitialization,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )
        start_time = time.monotonic()
        invocation_id = graph.run(block_until_done=True)
        duration_sec = time.monotonic() - start_time
        self.assertLess(
            duration_sec,
            20,  # Add extra for state reporting and reconciliation latency
            "Function initialization didn't timeout in duration close to 2 sec",
        )
        # Check that the function failed.
        output = graph.output(
            invocation_id, "FunctionThatSleepsForeverOnInitialization"
        )
        self.assertEqual(len(output), 0)

        # We don't assert of function's stderr right now due to Server side bugs with handling of stdouts.
        # It's good enough for now that it times out.

    def test_run(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_that_sleeps_forever_when_running,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )
        start_time = time.monotonic()
        invocation_id = graph.run(block_until_done=True)
        duration_sec = time.monotonic() - start_time
        self.assertLess(
            duration_sec,
            20,  # Add extra for state reporting and reconciliation latency
            "Function run didn't timeout in duration close to 3 sec",
        )
        # Check that the function failed.
        output = graph.output(
            invocation_id, "function_that_sleeps_forever_when_running"
        )
        self.assertEqual(len(output), 0)

        # We don't assert of function's stderr right now due to Server side bugs.
        # It's good enough for now that it times out.


if __name__ == "__main__":
    unittest.main()
