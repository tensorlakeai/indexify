import time
import unittest
from typing import List

from tensorlake import (
    Graph,
    tensorlake_function,
)
from testing import remote_or_local_graph, test_graph_name, wait_function_output

import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy

MAX_CONCURRENCY = 10
concurrency_counter: int = 0


@tensorlake.api()
@tensorlake.function(max_concurrency=MAX_CONCURRENCY)
def concurrent_function(_i: int) -> int:
    global concurrency_counter
    concurrency_counter += 1
    observed_max_concurrency = concurrency_counter
    # Simulate long IO bound work
    time.sleep(10)
    concurrency_counter -= 1
    return observed_max_concurrency


class TestFunctionConcurrency(unittest.TestCase):
    def test_function_reaches_max_concurrency(self):
        deploy(__file__)
        invocation_ids: List[tensorlake.Request] = []
        for _ in range(MAX_CONCURRENCY):
            request: tensorlake.Request = tensorlake.call_remote_api(concurrent_function, 0)
            invocation_ids.append(request)

        observed_max_concurrencies: List[int] = []
        for request in invocation_ids:
            output = request.output()
            observed_max_concurrencies.append(output)

        self.assertEqual(
            set(observed_max_concurrencies), set(range(1, MAX_CONCURRENCY + 1))
        )


if __name__ == "__main__":
    unittest.main()
