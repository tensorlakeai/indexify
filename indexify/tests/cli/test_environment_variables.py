import os
import subprocess
import unittest
from typing import Dict

import pydantic
from tensorlake.applications import Request, api, call_remote_api, function
from tensorlake.applications.remote.deploy import deploy
from testing import (
    ExecutorProcessContextManager,
    executor_pid,
    wait_executor_startup,
)


class Response(pydantic.BaseModel):
    executor_pid: int
    environment: Dict[str, str]


@api()
@function()
def function_a(_a: int) -> Response:
    return Response(executor_pid=executor_pid(), environment=os.environ.copy())


class TestEnvironmentVariables(unittest.TestCase):
    def setUp(self):
        deploy(__file__)

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

            for _ in range(10):
                request: Request = call_remote_api(
                    function_a,
                    1,
                )
                output: Response = request.output()
                if output.executor_pid == executor_a.pid:
                    print(
                        "The request landed on executor_a, verifying environment variables."
                    )
                    self.assertIn("INDEXIFY_TEST_ENV_VAR", output.environment)
                    self.assertEqual(
                        output.environment["INDEXIFY_TEST_ENV_VAR"], "test_value"
                    )
                    self.assertIn("INDEXIFY_TEST_ENV_VAR_2", output.environment)
                    self.assertEqual(
                        output.environment["INDEXIFY_TEST_ENV_VAR_2"], "test_value_2"
                    )
                    break


if __name__ == "__main__":
    unittest.main()
