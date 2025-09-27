import os
import platform
import unittest

import tensorlake.workflows.interface as tensorlake
from tensorlake.functions_sdk import graph
from tensorlake.workflows.remote.deploy import deploy


def function_executor_id() -> str:
    # PIDs are good for Subprocess Function Executors.
    # Hostnames are good for Function Executors running in VMs and containers.
    return str(os.getpid()) + str(platform.node())


@tensorlake.api()
@tensorlake.function()
def get_function_executor_id_1(_: int) -> str:
    return function_executor_id()


@tensorlake.api()
@tensorlake.function()
def get_function_executor_id_2(_: int) -> str:
    return function_executor_id()


class TestFunctionExecutorRouting(unittest.TestCase):
    def test_functions_of_same_app_version_run_in_same_function_executor(self):
        tensorlake.define_application(
            name="TestFunctionExecutorRouting.test_functions_of_same_app_version_run_in_same_function_executor"
        )
        deploy(__file__)

        request = tensorlake.call_remote_api(get_function_executor_id_1, 1)
        function_executor_id_1: str = request.output()

        request = tensorlake.call_remote_api(get_function_executor_id_1, 2)
        function_executor_id_2: str = request.output()

        self.assertEqual(function_executor_id_1, function_executor_id_2)

    def test_functions_of_different_app_versions_run_in_different_function_executors(
        self,
    ):
        APPLICATION_NAME = "TestFunctionExecutorRouting.test_functions_of_different_app_versions_run_in_different_function_executors"
        tensorlake.define_application(name=APPLICATION_NAME)
        deploy(__file__)

        request = tensorlake.call_remote_api(get_function_executor_id_1, 1)
        function_executor_id_1: str = request.output()

        # Creates a new random version
        tensorlake.define_application(name=APPLICATION_NAME)
        deploy(__file__)

        request = tensorlake.call_remote_api(get_function_executor_id_1, 1)
        function_executor_id_2: str = request.output()

        self.assertNotEqual(function_executor_id_1, function_executor_id_2)

    def test_different_functions_of_same_app_run_in_different_function_executors(
        self,
    ):
        APPLICATION_NAME = "TestFunctionExecutorRouting.test_different_functions_of_same_app_run_in_different_function_executors"
        tensorlake.define_application(name=APPLICATION_NAME)
        deploy(__file__)

        request = tensorlake.call_remote_api(get_function_executor_id_1, 1)
        function_executor_id_1: str = request.output()

        request = tensorlake.call_remote_api(get_function_executor_id_2, 2)
        function_executor_id_2: str = request.output()

        self.assertNotEqual(function_executor_id_1, function_executor_id_2)

    def test_same_functions_of_different_apps_run_in_different_function_executors(
        self,
    ):
        tensorlake.define_application(
            name="TestFunctionExecutorRouting.test_same_functions_of_different_apps_run_in_different_function_executors_app_1"
        )
        deploy(__file__)

        request = tensorlake.call_remote_api(get_function_executor_id_1, 1)
        function_executor_id_1: str = request.output()

        request = tensorlake.call_remote_api(get_function_executor_id_2, 2)
        function_executor_id_2: str = request.output()

        self.assertNotEqual(function_executor_id_1, function_executor_id_2)

    def test_same_functions_of_different_apps_run_in_different_function_executors(
        self,
    ):
        tensorlake.define_application(
            name="TestFunctionExecutorRouting.test_same_functions_of_different_apps_run_in_different_function_executors_app_1"
        )
        deploy(__file__)

        request = tensorlake.call_remote_api(get_function_executor_id_1, 1)
        function_executor_id_1: str = request.output()

        tensorlake.define_application(
            name="TestFunctionExecutorRouting.test_same_functions_of_different_apps_run_in_different_function_executors_app_2"
        )
        deploy(__file__)

        request = tensorlake.call_remote_api(get_function_executor_id_1, 1)
        function_executor_id_2: str = request.output()

        self.assertNotEqual(function_executor_id_1, function_executor_id_2)


if __name__ == "__main__":
    unittest.main()
