import time
import unittest

from tensorlake.applications import (
    Request,
    RequestFailed,
    application,
    cls,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications


@cls(init_timeout=2)
class FunctionThatSleepsForeverOnInitialization:
    def __init__(self):
        time.sleep(1000000)

    @application()
    @function()
    def run(self, _: str) -> str:
        raise Exception("This function can never run because it fails to initialize.")


@application()
@function(timeout=3)
def function_that_sleeps_forever_when_running(_: str) -> str:
    time.sleep(1000000)
    return "success"


class TestFunctionTimeouts(unittest.TestCase):
    def setUp(self):
        deploy_applications(__file__)

    def test_initilization(self):
        request: Request = run_remote_application(
            FunctionThatSleepsForeverOnInitialization.run,
            "",
        )
        start_time = time.monotonic()
        with self.assertRaises(RequestFailed) as cm:
            request.output()

        self.assertEqual(str(cm.exception), "function_timeout")
        duration_sec = time.monotonic() - start_time
        self.assertLess(
            duration_sec,
            20,  # Add extra for state reporting and reconciliation latency
            "Function initialization didn't timeout in duration close to 2 sec",
        )

    def test_run(self):
        request: Request = run_remote_application(
            function_that_sleeps_forever_when_running,
            "",
        )
        start_time = time.monotonic()
        with self.assertRaises(RequestFailed) as cm:
            request.output()

        self.assertEqual(str(cm.exception), "function_timeout")
        duration_sec = time.monotonic() - start_time
        self.assertLess(
            duration_sec,
            20,  # Add extra for state reporting and reconciliation latency
            "Function run didn't timeout in duration close to 3 sec",
        )


if __name__ == "__main__":
    unittest.main()
