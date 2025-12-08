import time
import unittest

from tensorlake.applications import (
    Request,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function()
def sleep_function(sleep_duration: float) -> str:
    time.sleep(sleep_duration)
    return "success"


class TestSlowAllocs(unittest.TestCase):
    def test_run(self):
        deploy_applications(__file__)

        requests: list[Request] = []
        for _ in range(4):
            request: Request = run_remote_application(sleep_function, 10.0)
            requests.append(request)

        for request in requests:
            self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
