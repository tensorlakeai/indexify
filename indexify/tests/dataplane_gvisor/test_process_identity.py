import os
import unittest
from typing import Any, List

from tensorlake.applications import (
    Request,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function()
def get_pid(_: str) -> int:
    return os.getpid()


@application()
@function()
def get_uid(_: Any) -> int:
    return os.getuid()


@application()
@function()
def get_gid(_: Any) -> int:
    return os.getgid()


@application()
@function()
def get_supplimentary_groups(_: Any) -> List[int]:
    return os.getgrouplist("root", 0)


@application()
@function()
def get_username(_: Any) -> str:
    return os.popen("whoami").read().strip()


class TestProcessIdentity(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        deploy_applications(__file__)

    def test_pid(self):
        request: Request = run_remote_application(get_pid, "")
        self.assertEqual(request.output(), 1)

    def test_uid(self):
        request: Request = run_remote_application(get_uid, "")
        self.assertEqual(request.output(), 0)

    def test_gid(self):
        request: Request = run_remote_application(get_gid, "")
        self.assertEqual(request.output(), 0)

    def test_supplementary_groups(self):
        request: Request = run_remote_application(get_supplimentary_groups, "")
        self.assertEqual(request.output(), [0])

    def test_username(self):
        request: Request = run_remote_application(get_username, "")
        self.assertEqual(request.output(), "root")


if __name__ == "__main__":
    unittest.main()
