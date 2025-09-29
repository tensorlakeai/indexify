import time
import unittest
from typing import List, Set

from tensorlake.applications import (
    Request,
    api,
    call_remote_api,
    define_application,
    function,
)
from tensorlake.applications.remote.deploy import deploy
from testing import function_executor_id


@api()
@function()
def test_function(sleep_secs: float) -> str:
    time.sleep(sleep_secs)
    return function_executor_id()


# Server side configuration.
_FE_ALLOCATIONS_QUEUE_SIZE = 1


class TestServerFunctionExecutorScaling(unittest.TestCase):
    def test_server_scales_up_function_executors_for_slow_function(self):
        # Use a different application in every test case to make sure that their FEs are independent
        # from each other.
        define_application(
            name="TestServerFunctionExecutorScaling.test_server_scales_up_function_executors_for_slow_function"
        )
        deploy(__file__)

        # The test runs a fixed number of long functions and checks that Server scaled
        # up an FE per function run because the functions are running and FE creations
        # for them are fast.
        # This requires at least 4 CPU cores and 4 GB of RAM on the testing machine.
        _EXPECTED_FE_COUNT = 4
        requests: List[Request] = []
        for _ in range(_EXPECTED_FE_COUNT * _FE_ALLOCATIONS_QUEUE_SIZE):
            request: Request = call_remote_api(
                test_function,
                5,
            )
            requests.append(request)

        fe_ids: Set[str] = set()
        for request in requests:
            request: Request
            fe_ids.add(request.output())

        self.assertEqual(len(fe_ids), _EXPECTED_FE_COUNT)

    def test_server_uses_the_same_function_executor_if_fe_task_queue_doesnt_overflow(
        self,
    ):
        # Use a different application in every test case to make sure that their FEs are independent
        # from each other.
        define_application(
            name="TestServerFunctionExecutorScaling.test_server_uses_the_same_function_executor_if_fe_task_queue_doesnt_overflow"
        )
        deploy(__file__)

        # The test runs a fixed number of fast functions and checks that Server reused
        # FEs because they never had their task queues full.
        requests: List[Request] = []
        for _ in range(_FE_ALLOCATIONS_QUEUE_SIZE):
            request: Request = call_remote_api(
                test_function,
                0.01,
            )
            requests.append(request)

        fe_ids: Set[str] = set()
        for request in requests:
            fe_ids.add(request.output())

        self.assertEqual(len(fe_ids), 1)


if __name__ == "__main__":
    unittest.main()
