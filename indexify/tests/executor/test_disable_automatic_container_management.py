import os
import platform
import subprocess
import time
import unittest

from tensorlake import Graph, tensorlake_function
from tensorlake.remote_graph import RemoteGraph
from testing import ExecutorProcessContextManager, test_graph_name


def function_executor_id() -> str:
    # PIDs are good for Subprocess Function Executors.
    # Hostnames are good for Function Executors running in VMs and containers.
    return str(os.getpid()) + str(platform.node())


@tensorlake_function()
def get_function_executor_id() -> str:
    return function_executor_id()


class TestDisabledAutomaticContainerManagement(unittest.TestCase):
    def test_two_executors_only_one_function_executor_destroy(self):
        graph_v1 = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=get_function_executor_id,
            version="1.0",
        )
        graph_v1 = RemoteGraph.deploy(graph_v1)

        invocation_id = graph_v1.run(block_until_done=True)
        output = graph_v1.output(invocation_id, "get_function_executor_id")
        self.assertEqual(len(output), 1)
        destroyable_fe_id = output[0]

        with ExecutorProcessContextManager(
            [
                "--disable-automatic-function-executor-management",
                "--dev",
                "--ports",
                "60000",
                "60001",
            ]
        ) as executor_a:
            executor_a: subprocess.Popen
            print(f"Started Executor A with PID: {executor_a.pid}")
            print("Waiting 5 secs for Executors A to start and join the Server.")
            time.sleep(5)

            # As invocations might land on dev Executor, we need to run the graph multiple times
            # to ensure that we catch function executor getting destroyed on Executor A if it ever happens.
            fe_ids_v1 = set()
            for _ in range(10):
                invocation_id = graph_v1.run(block_until_done=True)
                output = graph_v1.output(invocation_id, "get_function_executor_id")
                self.assertEqual(len(output), 1)
                fe_ids_v1.add(output[0])

            not_destroyable_fe_id = None
            self.assertLessEqual(len(fe_ids_v1), 2)
            if len(fe_ids_v1) == 2:
                self.assertIn(destroyable_fe_id, fe_ids_v1)
                not_destroyable_fe_id = fe_ids_v1.difference([destroyable_fe_id]).pop()

            graph_v2 = Graph(
                name=test_graph_name(self),
                description="test",
                start_node=get_function_executor_id,
                version="2.0",
            )
            graph_v2 = RemoteGraph.deploy(graph_v2)

            fe_ids_v2 = set()
            for _ in range(10):
                invocation_id = graph_v2.run(block_until_done=True)
                output = graph_v2.output(invocation_id, "get_function_executor_id")
                self.assertEqual(len(output), 1)
                fe_ids_v2.add(output[0])

            self.assertLessEqual(len(fe_ids_v2), 2)
            self.assertTrue(destroyable_fe_id not in fe_ids_v2)
            if len(fe_ids_v2) == 2 and not_destroyable_fe_id is not None:
                self.assertIn(not_destroyable_fe_id, fe_ids_v2)


if __name__ == "__main__":
    unittest.main()
