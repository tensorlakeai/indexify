import time
import unittest
from typing import List

from tensorlake.applications import (
    Request,
    api,
    call_remote_api,
    define_application,
    function,
)
from tensorlake.applications.remote.deploy import deploy

MAX_CONCURRENCY = 10
concurrency_counter: int = 0


@api()
@function(max_concurrency=MAX_CONCURRENCY)
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
        requests: List[Request] = []
        for _ in range(MAX_CONCURRENCY):
            request: Request = call_remote_api(concurrent_function, 0)
            requests.append(request)

        observed_max_concurrencies: List[int] = []
        for request in requests:
            observed_max_concurrencies.append(request.output())

        self.assertEqual(
            set(observed_max_concurrencies), set(range(1, MAX_CONCURRENCY + 1))
        )


if __name__ == "__main__":
    unittest.main()
