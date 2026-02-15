import os
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
def get_cwd_path(_: str) -> str:
    return os.getcwd()


@application()
@function()
def path_exists(path: str) -> bool:
    return os.path.exists(path)


class TestWorkingDirectory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy_applications(__file__)

    def test_expected_working_directory(self):
        request: Request = run_remote_application(get_cwd_path, "")
        self.assertEqual(request.output(), "/app")

    def test_expected_working_directory_exists(self):
        request: Request = run_remote_application(path_exists, "/app")
        self.assertTrue(request.output())


if __name__ == "__main__":
    unittest.main()
