import subprocess
import time
import unittest
from typing import List

from tensorlake import Graph, tensorlake_function
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
from tensorlake.remote_graph import RemoteGraph
from testing import (
    ExecutorProcessContextManager,
    function_uri,
    test_graph_name,
    wait_executor_startup,
    wait_function_output,
)


@tensorlake_function()
def success_func(sleep_secs: float) -> str:
    time.sleep(sleep_secs)
    return "success"


class TestExecutorExit(unittest.TestCase):
    def test_all_tasks_succeed_when_executor_exits(self):
        graph_name = test_graph_name(self)
        version = str(time.time())

        with ExecutorProcessContextManager(
            [
                "--function",
                function_uri("default", graph_name, "success_func", version),
                "--monitoring-server-port",
                "7001",
            ],
            keep_std_outputs=True,
        ) as executor_a:
            executor_a: subprocess.Popen
            print(f"Started Executor A with PID: {executor_a.pid}")
            wait_executor_startup(7001)

            graph = Graph(
                name=graph_name,
                description="test",
                start_node=success_func,
                version=version,
            )
            graph = RemoteGraph.deploy(
                graph=graph, code_dir_path=graph_code_dir_path(__file__)
            )

            invocation_ids: List[str] = []
            for i in range(10):
                print(f"Running invocation {i}")
                invocation_id = graph.run(block_until_done=False, sleep_secs=0.1)
                invocation_ids.append(invocation_id)

        print("Waiting for all invocations to finish...")
        for invocation_id in invocation_ids:
            print(f"Waiting for invocation {invocation_id} to finish...")
            output = wait_function_output(graph, invocation_id, "success_func")
            print(f"output for {invocation_id}: {output}")
            self.assertEqual(len(output), 1)
            self.assertEqual(output[0], "success")


if __name__ == "__main__":
    unittest.main()
