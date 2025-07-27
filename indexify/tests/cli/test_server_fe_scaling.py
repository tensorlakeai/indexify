import time
import unittest
from typing import List, Set

from tensorlake import Graph, TensorlakeCompute
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
from tensorlake.functions_sdk.remote_graph import RemoteGraph
from testing import (
    function_executor_id,
    test_graph_name,
    wait_function_output,
)


class TestFunction(TensorlakeCompute):
    name = "test_function"

    def __init__(self):
        pass

    def run(self, sleep_secs: float) -> str:
        time.sleep(sleep_secs)
        return function_executor_id()


# Server side configuration.
_FE_ALLOCATIONS_QUEUE_SIZE = 2


class TestServerFunctionExecutorScaling(unittest.TestCase):
    def test_server_scales_up_function_executors_for_slow_function(self):
        # The test runs a fixed number of long functions and checks that Server scaled
        # up an FE per function run because the functions are running and FE creations
        # for them are fast.
        graph_name = test_graph_name(self)
        version = str(time.time())

        # This requires at least 4 CPU cores and 4 GB of RAM on the testing machine.
        _EXPECTED_FE_COUNT = 4
        graph = Graph(
            name=graph_name,
            description="test",
            start_node=TestFunction,
            version=version,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )

        invocation_ids: List[str] = []
        for _ in range(_EXPECTED_FE_COUNT * _FE_ALLOCATIONS_QUEUE_SIZE):
            invocation_id = graph.run(block_until_done=False, sleep_secs=5)
            invocation_ids.append(invocation_id)

        fe_ids: Set[str] = set()
        for invocation_id in invocation_ids:
            output = wait_function_output(graph, invocation_id, TestFunction.name)
            self.assertEqual(len(output), 1)
            fe_ids.add(output[0])

        self.assertEqual(len(fe_ids), _EXPECTED_FE_COUNT)

    def test_server_uses_the_same_function_executor_if_fe_task_queue_doesnt_overflow(
        self,
    ):
        # The test runs a fixed number of fast functions and checks that Server reused
        # FEs because they never had their task queues full.
        graph_name = test_graph_name(self)
        version = str(time.time())

        graph = Graph(
            name=graph_name,
            description="test",
            start_node=TestFunction,
            version=version,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )

        invocation_ids: List[str] = []
        for _ in range(_FE_ALLOCATIONS_QUEUE_SIZE):
            invocation_id = graph.call(sleep_secs=0.01)
            invocation_ids.append(invocation_id)

        fe_ids: Set[str] = set()
        for invocation_id in invocation_ids:
            output = wait_function_output(graph, invocation_id, TestFunction.name)
            self.assertEqual(len(output), 1)
            fe_ids.add(output[0])

        self.assertEqual(len(fe_ids), 1)


if __name__ == "__main__":
    unittest.main()
