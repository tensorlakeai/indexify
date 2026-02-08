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

MAX_CONCURRENCY = 10
concurrency_counter: int = 0


@application()
# @function(max_concurrency=MAX_CONCURRENCY)
@function()
def concurrent_function(_i: int) -> int:
    global concurrency_counter
    concurrency_counter += 1
    observed_max_concurrency = concurrency_counter
    # Simulate long IO bound work
    time.sleep(10)
    concurrency_counter -= 1
    return observed_max_concurrency


# A workaround to set max_concurrency since it's hidden right now in SDK interface
# because Telemetry doesn't support concurrent allocation runs yet.
concurrent_function._function_config.max_concurrency = MAX_CONCURRENCY


class TestFunctionConcurrency(unittest.TestCase):
    def test_function_reaches_max_concurrency(self):
        deploy_applications(__file__)
        requests: List[Request] = []
        for _ in range(MAX_CONCURRENCY):
            request: Request = run_remote_application(concurrent_function, 0)
            requests.append(request)

        observed_max_concurrencies: List[int] = []
        for request in requests:
            observed_max_concurrencies.append(request.output())

        # Verify that max concurrency was reached. We don't require every
        # level 1..MAX_CONCURRENCY to be observed because the global counter
        # is not thread-safe and concurrent increments can skip levels.
        self.assertEqual(max(observed_max_concurrencies), MAX_CONCURRENCY)


if __name__ == "__main__":
    unittest.main()
