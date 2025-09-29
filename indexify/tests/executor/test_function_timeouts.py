import time
import unittest

from tensorlake.applications import (
    Request,
    RequestFailureException,
    api,
    call_remote_api,
    cls,
    function,
)
from tensorlake.applications.remote.deploy import deploy


@cls(init_timeout=2)
class FunctionThatSleepsForeverOnInitialization:
    def __init__(self):
        time.sleep(1000000)

    @api()
    @function()
    def run(self, _: str) -> str:
        raise Exception("This function can never run because it fails to initialize.")


@api()
@function(timeout=3)
def function_that_sleeps_forever_when_running(_: str) -> str:
    time.sleep(1000000)
    return "success"


class TestFunctionTimeouts(unittest.TestCase):
    def setUp(self):
        deploy(__file__)

    def test_initilization(self):
        request: Request = call_remote_api(
            FunctionThatSleepsForeverOnInitialization.run,
            "",
        )
        start_time = time.monotonic()
        # Check that the function failed.
        self.assertRaises(RequestFailureException, request.output)
        duration_sec = time.monotonic() - start_time
        self.assertLess(
            duration_sec,
            20,  # Add extra for state reporting and reconciliation latency
            "Function initialization didn't timeout in duration close to 2 sec",
        )

    def test_run(self):
        request: Request = call_remote_api(
            function_that_sleeps_forever_when_running,
            "",
        )
        start_time = time.monotonic()
        # Check that the function failed.
        self.assertRaises(RequestFailureException, request.output)
        duration_sec = time.monotonic() - start_time
        self.assertLess(
            duration_sec,
            20,  # Add extra for state reporting and reconciliation latency
            "Function run didn't timeout in duration close to 3 sec",
        )


if __name__ == "__main__":
    unittest.main()
