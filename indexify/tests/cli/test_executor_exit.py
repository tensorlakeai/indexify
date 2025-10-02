import subprocess
import time
import unittest
from typing import List

from tensorlake.applications import (
    Request,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications
from testing import (
    ExecutorProcessContextManager,
    function_uri,
    wait_executor_startup,
)


@application()
@function()
def success_func(sleep_secs: float) -> str:
    time.sleep(sleep_secs)
    return "success"


class TestExecutorExit(unittest.TestCase):
    def setUp(self):
        deploy_applications(__file__)

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

            requests: List[Request] = []
            for i in range(10):
                print(f"Running request {i}")
                request: Request = run_remote_application(
                    success_func,
                    0.1,
                )
                requests.append(request)

        print("Waiting for all requests to finish...")
        for request in requests:
            print(f"Waiting for request {request.id} to finish...")
            output: str = request.output()
            print(f"output for {request.id}: {output}")
            self.assertEqual(output, "success")


if __name__ == "__main__":
    unittest.main()
