import os
from typing import Dict

import pydantic
from tensorlake.applications import (
    Request,
    application,
    function,
    run_remote_application,
)


class EnvResponse(pydantic.BaseModel):
    environment: Dict[str, str]


@application()
@function(region="env-test-region")
def get_environment(_a: int) -> EnvResponse:
    return EnvResponse(environment=dict(os.environ))


# Test infrastructure imports are guarded so they don't execute when the
# function executor loads this file as an application module.
if __name__ == "__main__":
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    import unittest
    from tensorlake.applications.remote.deploy import deploy_applications
    from dataplane_cli.testing import (
        DataplaneProcessContextManager,
        find_free_port,
        wait_dataplane_startup,
    )

    class TestEnvironmentVariablesDataplane(unittest.TestCase):
        def setUp(self):
            deploy_applications(__file__)

        def test_executor_env_variables_are_passed_to_functions(self):
            """Verify that extra_env variables on the dataplane process are
            inherited by function executor subprocesses."""
            port = find_free_port()
            with DataplaneProcessContextManager(
                config_overrides={"http_proxy": {"port": port}},
                keep_std_outputs=True,
                labels={"region": "env-test-region"},
                extra_env={
                    "INDEXIFY_TEST_ENV_VAR": "test_value",
                    "INDEXIFY_TEST_ENV_VAR_2": "test_value_2",
                },
            ) as proc:
                print(f"Started dataplane with PID: {proc.pid}")
                wait_dataplane_startup(port)

                request: Request = run_remote_application(get_environment, 1)
                output: EnvResponse = request.output()

                self.assertIn("INDEXIFY_TEST_ENV_VAR", output.environment)
                self.assertEqual(
                    output.environment["INDEXIFY_TEST_ENV_VAR"], "test_value"
                )
                self.assertIn("INDEXIFY_TEST_ENV_VAR_2", output.environment)
                self.assertEqual(
                    output.environment["INDEXIFY_TEST_ENV_VAR_2"], "test_value_2"
                )
                print("Environment variables verified successfully.")

    unittest.main()
