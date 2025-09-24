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

import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy

@tensorlake.cls()
class FunctionThatSleepsForeverOnInitialization(TensorlakeCompute):
    name = "FunctionThatSleepsForeverOnInitialization"
    timeout = 2

    def __init__(self):
        super().__init__()
        time.sleep(1000000)

    @tensorlake.api()
    @tensorlake.function()
    def run(self, action: str) -> str:
        raise Exception("This function can never run because it fails to initialize.")


@tensorlake.api()
@tensorlake.function(timeout=3)
def function_that_sleeps_forever_when_running() -> str:
    time.sleep(1000000)
    return "success"


class TestFunctionTimeouts(unittest.TestCase):
    def setUp(self):
        deploy(__file__)

    def test_initilization(self):
        request: tensorlake.Request = tensorlake.call_remote_api(
            FunctionThatSleepsForeverOnInitialization,
            "action",
        )
        start_time = time.monotonic()
        duration_sec = time.monotonic() - start_time
        self.assertLess(
            duration_sec,
            20,  # Add extra for state reporting and reconciliation latency
            "Function initialization didn't timeout in duration close to 2 sec",
        )
        # Check that the function failed.
        try:
            output = request.output()
        except Exception as e:
            from tensorlake.workflows.interface.exceptions import RequestFailureException
            self.assertTrue(isinstance(e, RequestFailureException))
            self.assertEqual(e.message, "This function can never run because it fails to initialize.")

        # We don't assert of function's stderr right now due to Server side bugs with handling of stdouts.
        # It's good enough for now that it times out.

    def test_run(self):
        start_time = time.monotonic()
        request: tensorlake.Request = tensorlake.call_remote_api(
            function_that_sleeps_forever_when_running,
            "action",
        )
        duration_sec = time.monotonic() - start_time
        self.assertLess(
            duration_sec,
            20,  # Add extra for state reporting and reconciliation latency
            "Function run didn't timeout in duration close to 3 sec",
        )
        # Check that the function failed.
        try:
            output = request.output()
        except Exception as e:
            from tensorlake.workflows.interface.exceptions import RequestFailureException
            self.assertTrue(isinstance(e, RequestFailureException))

        # We don't assert of function's stderr right now due to Server side bugs.
        # It's good enough for now that it times out.


if __name__ == "__main__":
    unittest.main()
