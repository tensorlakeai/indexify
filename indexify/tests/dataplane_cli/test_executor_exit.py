import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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
from cli.testing import function_uri
from dataplane_cli.testing import (
    DataplaneProcessContextManager,
    wait_dataplane_startup,
)


@application()
@function(labels={"exit-test": "true"})
def success_func(sleep_secs: float) -> str:
    time.sleep(sleep_secs)
    return "success"


class TestExecutorExitDataplane(unittest.TestCase):
    def setUp(self):
        deploy_applications(__file__)

    def test_all_tasks_succeed_when_executor_exits(self):
        version = str(time.time())

        # The function URI format expected by the server/dataplane
        # namespace:application:function[:version]
        # function_uri helper produces this.
        fn_uri = function_uri("default", "success_func", version)

        with DataplaneProcessContextManager(
            config_overrides={"http_proxy": {"port": 7001}},
            labels={"exit-test": "true"},
            args=["--function", fn_uri],
            keep_std_outputs=True,
        ) as executor_a:
            executor_a: subprocess.Popen
            print(f"Started Executor A with PID: {executor_a.pid}")
            wait_dataplane_startup(7001)

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
