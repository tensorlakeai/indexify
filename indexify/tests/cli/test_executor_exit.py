import subprocess
import time
import unittest
from typing import List

from testing import (
    ExecutorProcessContextManager,
    function_uri,
    wait_executor_startup,
)
import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy

@tensorlake.api()
@tensorlake.function()
def success_func(sleep_secs: float) -> str:
    time.sleep(sleep_secs)
    return "success"


class TestExecutorExit(unittest.TestCase):
    def setUp(self):
        deploy(__file__)

    def test_all_tasks_succeed_when_executor_exits(self):
        version = str(time.time())

        with ExecutorProcessContextManager(
            [
                "--function",
                function_uri("default", "success_func", version),
                "--monitoring-server-port",
                "7001",
            ],
            keep_std_outputs=True,
        ) as executor_a:
            executor_a: subprocess.Popen
            print(f"Started Executor A with PID: {executor_a.pid}")
            wait_executor_startup(7001)

            invocation_ids: List[str] = []
            for i in range(10):
                print(f"Running invocation {i}")
                request: tensorlake.Request = tensorlake.call_remote_api(
                    success_func,
                    0.1,
                )
                invocation_ids.append(request.id)

        print("Waiting for all invocations to finish...")
        for invocation_id in invocation_ids:
            print(f"Waiting for invocation {invocation_id} to finish...")
            output = request.output()
            print(f"output for {invocation_id}: {output}")
            self.assertEqual(output, "success")


if __name__ == "__main__":
    unittest.main()
