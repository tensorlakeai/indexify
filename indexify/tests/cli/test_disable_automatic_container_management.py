import os
import platform
import subprocess
import unittest

from tensorlake import Graph, tensorlake_function
from tensorlake.remote_graph import RemoteGraph
from testing import (
    ExecutorProcessContextManager,
    test_graph_name,
    wait_executor_startup,
)


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
                "--monitoring-server-port",
                "7001",
            ]
        ) as executor_a:
            executor_a: subprocess.Popen
            print(f"Started Executor A with PID: {executor_a.pid}")
            wait_executor_startup(7001)

            # As invocations might land on dev Executor, we need to run the graph multiple times
            # to ensure that we catch function executor getting destroyed on Executor A if it ever happens.
            fe_ids_v1 = []
            for _ in range(10):
                invocation_id = graph_v1.run(block_until_done=True)
                output = graph_v1.output(invocation_id, "get_function_executor_id")
                self.assertEqual(len(output), 1)
                if output[0] not in fe_ids_v1:
                    fe_ids_v1.append(output[0])

            self.assertGreaterEqual(len(fe_ids_v1), 1)

            graph_v2 = Graph(
                name=test_graph_name(self),
                description="test",
                start_node=get_function_executor_id,
                version="2.0",
            )
            graph_v2 = RemoteGraph.deploy(graph_v2)

            success_fe_ids_v2 = []
            for _ in range(10):
                invocation_id = graph_v2.run(block_until_done=True)
                output = graph_v2.output(invocation_id, "get_function_executor_id")
                if len(output) == 1 and output[0] not in success_fe_ids_v2:
                    success_fe_ids_v2.append(output[0])

            # Executor A should fail in all v2 invokes because it won't destroy its function executor and
            # the function executor will raise an error because the task version must be v1 not v2.
            self.assertEqual(len(success_fe_ids_v2), 1)


if __name__ == "__main__":
    unittest.main()
