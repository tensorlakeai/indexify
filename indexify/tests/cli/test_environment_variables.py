import os
import subprocess
import unittest
from typing import Dict

import pydantic
import testing
from tensorlake import Graph, RemoteGraph, tensorlake_function
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
from testing import (
    ExecutorProcessContextManager,
    executor_pid,
    test_graph_name,
    wait_executor_startup,
)


class Response(pydantic.BaseModel):
    executor_pid: int
    environment: Dict[str, str]


@tensorlake_function()
def function_a() -> Response:
    return Response(executor_pid=executor_pid(), environment=os.environ.copy())


class TestEnvironmentVariables(unittest.TestCase):
    def test_executor_env_variables_are_passed_to_functions(self):
        with ExecutorProcessContextManager(
            [
                "--monitoring-server-port",
                "7001",
            ],
            keep_std_outputs=False,
            extra_env={
                "INDEXIFY_TEST_ENV_VAR": "test_value",
                "INDEXIFY_TEST_ENV_VAR_2": "test_value_2",
            },
        ) as executor_a:
            executor_a: subprocess.Popen
            print(f"Started Executor A with PID: {executor_a.pid}")
            wait_executor_startup(7001)

            graph = Graph(
                name=test_graph_name(self),
                description="test",
                start_node=function_a,
            )
            graph = RemoteGraph.deploy(
                graph=graph, code_dir_path=graph_code_dir_path(__file__)
            )

            # Run 10 times to have close to 100% chance of landing the functions on executor_a and not default test executor.
            for _ in range(10):
                invocation_id = graph.run(block_until_done=True)
                output = graph.output(invocation_id, "function_a")
                self.assertEqual(len(output), 1)
                response: Response = output[0]
                if response.executor_pid == executor_a.pid:
                    print(
                        "The invocation landed on executor_a, verifying environment variables."
                    )
                    self.assertIn("INDEXIFY_TEST_ENV_VAR", response.environment)
                    self.assertEqual(
                        response.environment["INDEXIFY_TEST_ENV_VAR"], "test_value"
                    )
                    self.assertIn("INDEXIFY_TEST_ENV_VAR_2", response.environment)
                    self.assertEqual(
                        response.environment["INDEXIFY_TEST_ENV_VAR_2"], "test_value_2"
                    )
                    break


if __name__ == "__main__":
    unittest.main()
