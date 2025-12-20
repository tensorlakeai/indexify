import time
import unittest

from tensorlake.applications import (
    Future,
    Request,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function(timeout=2)
def blocking_function_1(_: int) -> str:
    # The call chain will fail if child function call
    # timeouts don't propagate to parent because the overall
    # chaing takes more than 5 seconds.
    return blocking_function_2()


@function(timeout=2)
def blocking_function_2() -> str:
    return blocking_function_3()


@function(timeout=10)
def blocking_function_3() -> str:
    time.sleep(5)
    return "success"


class TestFunctionTimeoutPropagationWithBlockingCalls(unittest.TestCase):
    def setUp(self):
        deploy_applications(__file__)

    def test_function_timeouts_propagate(self):
        request: Request = run_remote_application(blocking_function_1, 1)
        self.assertEqual(request.output(), "success")


@application()
@function(timeout=2)
def future_function_1(_: int) -> str:
    # The call chain will fail if child function call
    # timeouts don't propagate to parent because the overall
    # chaing takes more than 5 seconds.
    return future_function_2.awaitable().run().result()


@function(timeout=2)
def future_function_2() -> str:
    return future_function_3.awaitable().run().result()


@function(timeout=10)
def future_function_3() -> str:
    time.sleep(5)
    return "success"


class TestFunctionTimeoutPropagationWithFutureCalls(unittest.TestCase):
    def setUp(self):
        deploy_applications(__file__)

    def test_function_timeouts_propagate(self):
        request: Request = run_remote_application(future_function_1, 1)
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
