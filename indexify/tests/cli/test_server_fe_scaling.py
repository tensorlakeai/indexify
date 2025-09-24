import time
import unittest
from typing import List, Set

from testing import function_executor_id
import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy

@tensorlake.api()
@tensorlake.function()
def test_function(sleep_secs: float) -> str:
    time.sleep(sleep_secs)
    return function_executor_id()

# Server side configuration.
_FE_ALLOCATIONS_QUEUE_SIZE = 2

class TestServerFunctionExecutorScaling(unittest.TestCase):
    def setUp(self):
        deploy(__file__)

    def test_server_scales_up_function_executors_for_slow_function(self):
        # The test runs a fixed number of long functions and checks that Server scaled
        # up an FE per function run because the functions are running and FE creations
        # for them are fast.
        version = str(time.time())

        # This requires at least 4 CPU cores and 4 GB of RAM on the testing machine.
        _EXPECTED_FE_COUNT = 4
        invocation_ids: List[tensorlake.Request] = []
        for _ in range(_EXPECTED_FE_COUNT * _FE_ALLOCATIONS_QUEUE_SIZE):
            request: tensorlake.Request = tensorlake.call_remote_api(
                test_function,
                5,
            )
            invocation_ids.append(request)

        fe_ids: Set[str] = set()
        for request in invocation_ids:
            output = request.output()
            fe_ids.add(output)

        self.assertEqual(len(fe_ids), _EXPECTED_FE_COUNT)

    def test_server_uses_the_same_function_executor_if_fe_task_queue_doesnt_overflow(
        self,
    ):
        # The test runs a fixed number of fast functions and checks that Server reused
        # FEs because they never had their task queues full.
        version = str(time.time())

        invocation_ids: List[tensorlake.Request] = []
        for _ in range(_FE_ALLOCATIONS_QUEUE_SIZE):
            request: tensorlake.Request = tensorlake.call_remote_api(
                test_function,
                0.01,
            )
            invocation_ids.append(request)

        fe_ids: Set[str] = set()
        for request in invocation_ids:
            output = request.output()
            fe_ids.add(output)

        self.assertEqual(len(fe_ids), 1)


if __name__ == "__main__":
    unittest.main()
