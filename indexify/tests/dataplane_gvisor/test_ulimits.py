import resource
import unittest
from typing import Dict, List

from tensorlake.applications import (
    Request,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function()
def get_ulimits(_: str) -> Dict[str, List[int]]:
    limits = {}
    resources = [
        ("core", resource.RLIMIT_CORE),
        ("stack", resource.RLIMIT_STACK),
        ("memlock", resource.RLIMIT_MEMLOCK),
        ("nofile", resource.RLIMIT_NOFILE),
        ("msgqueue", resource.RLIMIT_MSGQUEUE),
    ]
    for res in resources:
        soft, hard = resource.getrlimit(res[1])
        limits[res[0]] = [soft, hard]
    return limits


class TestUlimits(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy_applications(__file__)

    def test_expected_ulimits(self):
        request: Request = run_remote_application(get_ulimits, "")
        ONE_GB = 1024 * 1024 * 1024
        ONE_MILLION = 1000000
        self.assertEqual(
            request.output(),
            {
                "core": [0, 0],
                "stack": [ONE_GB, ONE_GB],
                "memlock": [ONE_GB, ONE_GB],
                "nofile": [ONE_MILLION, ONE_MILLION],
                "msgqueue": [ONE_GB, ONE_GB],
            },
        )


if __name__ == "__main__":
    unittest.main()
