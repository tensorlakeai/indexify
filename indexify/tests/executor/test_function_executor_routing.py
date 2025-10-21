import os
import platform
import unittest

from nanoid import generate as nanoid_generate
from tensorlake.applications import (
    Function,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications


def function_executor_id() -> str:
    # PIDs are good for Subprocess Function Executors.
    # Hostnames are good for Function Executors running in VMs and containers.
    return str(os.getpid()) + str(platform.node())


@application()
@function()
def get_function_executor_id_1(_: int) -> str:
    return function_executor_id()


@application()
@function()
def get_function_executor_id_2(_: int) -> str:
    return function_executor_id()


def update_random_version(func: Function) -> None:
    # Hacky way to update application version.
    func._application_config.version = nanoid_generate()


class TestFunctionExecutorRouting(unittest.TestCase):
    def test_same_functions_of_same_app_version_run_in_same_function_executor(self):
        update_random_version(get_function_executor_id_1)
        deploy_applications(__file__)

        request = run_remote_application(get_function_executor_id_1, 1)
        function_executor_id_1: str = request.output()

        request = run_remote_application(get_function_executor_id_1, 2)
        function_executor_id_2: str = request.output()

        self.assertEqual(function_executor_id_1, function_executor_id_2)

    def test_same_functions_of_different_app_versions_run_in_different_function_executors(
        self,
    ):
        update_random_version(get_function_executor_id_1)
        deploy_applications(__file__)

        request = run_remote_application(get_function_executor_id_1, 1)
        function_executor_id_1: str = request.output()

        update_random_version(get_function_executor_id_1)
        deploy_applications(__file__)

        request = run_remote_application(get_function_executor_id_1, 1)
        function_executor_id_2: str = request.output()

        self.assertNotEqual(function_executor_id_1, function_executor_id_2)

    def test_different_functions_of_different_apps_run_in_different_function_executors(
        self,
    ):
        deploy_applications(__file__)

        request = run_remote_application(get_function_executor_id_1, 1)
        function_executor_id_1: str = request.output()

        request = run_remote_application(get_function_executor_id_2, 2)
        function_executor_id_2: str = request.output()

        self.assertNotEqual(function_executor_id_1, function_executor_id_2)


if __name__ == "__main__":
    unittest.main()
