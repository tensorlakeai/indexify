import os

from tensorlake.applications import (
    Request,
    application,
    function,
    run_remote_application,
)


def executor_pid() -> int:
    """Returns the PID of the executor (dataplane) running this function.

    In the fork_exec driver, the dataplane forks a function-executor subprocess
    which runs user code in-process, so os.getppid() returns the dataplane PID.
    """
    return os.getppid()


@application()
@function()
def get_dev_mode_executor_pid(_: int) -> int:
    """Returns the PID of the executor running this function."""
    return executor_pid()


@application()
@function(region="us-east-1")
def regional_function_east(_: int) -> int:
    """Function that requires us-east-1 region."""
    return executor_pid()


@application()
@function(region="us-west-2")
def regional_function_west(_: int) -> int:
    """Function that requires us-west-2 region."""
    return executor_pid()


@application()
@function(region="eu-west-1")
def regional_function_eu(_: int) -> int:
    """Function that requires eu-west-1 region."""
    return executor_pid()


@application()
@function(region="us-east-1")
def regional_function_east_2(_: int) -> int:
    """Function that requires us-east-1 region."""
    return executor_pid()


@application()
@function(region="us-west-2")
def regional_function_west_2(_: int) -> int:
    """Function that requires us-west-2 region."""
    return executor_pid()


# Test infrastructure imports are guarded so they don't execute when the
# function executor loads this file as an application module.
if __name__ == "__main__":
    import contextlib
    import sys
    import unittest
    from typing import Dict, List

    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    from tensorlake.applications.remote.deploy import deploy_applications
    from dataplane_cli.testing import (
        DataplaneProcessContextManager,
        find_free_port,
        wait_dataplane_startup,
    )

    class TestRegionalRoutingDataplane(unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            deploy_applications(__file__)

        def test_regional_routing(self):
            """Test that functions are routed to executors in the correct regions."""

            # Validate that functions with regional constraints only run on
            # executors in the matching region. We start a dev-mode executor
            # (no label restrictions) plus three regional executors, then verify
            # that each regional function consistently lands on the executor
            # with the matching region label.

            NUM_REQUESTS_PER_REGION = 5

            executor_configs: List[dict] = [
                {
                    "name": "dev_mode",
                    "labels": {},
                    "port": find_free_port(),
                },
                {
                    "name": "us_east_1",
                    "labels": {"sku": "cpu-s", "region": "us-east-1"},
                    "port": find_free_port(),
                },
                {
                    "name": "us_west_2",
                    "labels": {"sku": "gpu-xl", "region": "us-west-2"},
                    "port": find_free_port(),
                },
                {
                    "name": "eu_west_1",
                    "labels": {"sku": "gpu-xxl", "region": "eu-west-1"},
                    "port": find_free_port(),
                },
            ]

            executor_cms = [
                DataplaneProcessContextManager(
                    config_overrides={"http_proxy": {"port": config["port"]}},
                    keep_std_outputs=True,
                    labels=config["labels"],
                )
                for config in executor_configs
            ]

            executor_to_pid: Dict[str, int] = {}

            with contextlib.ExitStack() as stack:
                pid_to_executor_name: Dict[int, str] = {}
                for cm, config in zip(executor_cms, executor_configs):
                    proc = stack.enter_context(cm)
                    executor_to_pid[config["name"]] = proc.pid
                    pid_to_executor_name[proc.pid] = config["name"]
                    print(
                        f"Started {config['name']} executor (PID: {proc.pid}, port: {config['port']}) with labels: {config['labels']}"
                    )

                for config in executor_configs:
                    wait_dataplane_startup(config["port"])
                    print(f"Executor {config['name']} is ready")

                # Run multiple requests to test label filtering
                regional_function_requests: Dict[str, List[Request]] = {
                    "regional_function_east": [],
                    "regional_function_west": [],
                    "regional_function_eu": [],
                    "regional_function_east_2": [],
                    "regional_function_west_2": [],
                }
                for _ in range(NUM_REQUESTS_PER_REGION):
                    regional_function_requests["regional_function_east"].append(
                        run_remote_application(regional_function_east, 0)
                    )
                    regional_function_requests["regional_function_west"].append(
                        run_remote_application(regional_function_west, 0)
                    )
                    regional_function_requests["regional_function_eu"].append(
                        run_remote_application(regional_function_eu, 0)
                    )
                    regional_function_requests["regional_function_east_2"].append(
                        run_remote_application(regional_function_east_2, 0)
                    )
                    regional_function_requests["regional_function_west_2"].append(
                        run_remote_application(regional_function_west_2, 0)
                    )

                print(
                    f"Running {NUM_REQUESTS_PER_REGION * len(regional_function_requests)} requests..."
                )

                # Track which executors ran each function
                regional_function_to_executor_pids: Dict[str, List[int]] = {
                    "regional_function_east": [],
                    "regional_function_west": [],
                    "regional_function_eu": [],
                    "regional_function_east_2": [],
                    "regional_function_west_2": [],
                }

                for func_name, requests in regional_function_requests.items():
                    for request in requests:
                        pid = request.output()
                        regional_function_to_executor_pids[func_name].append(pid)

                function_name_to_allowed_executor_pid: Dict[str, int] = {
                    "regional_function_east": executor_to_pid["us_east_1"],
                    "regional_function_west": executor_to_pid["us_west_2"],
                    "regional_function_eu": executor_to_pid["eu_west_1"],
                    "regional_function_east_2": executor_to_pid["us_east_1"],
                    "regional_function_west_2": executor_to_pid["us_west_2"],
                }

                for func_name, pids in regional_function_to_executor_pids.items():
                    func_allowed_pid: int = function_name_to_allowed_executor_pid[func_name]
                    for pid in pids:
                        self.assertEqual(
                            pid,
                            func_allowed_pid,
                            f"Function {func_name} ran on wrong executor (PID {pid}, expected {func_allowed_pid} ({pid_to_executor_name[func_allowed_pid]}))",
                        )

                print("All regional routing assertions passed!")

    unittest.main()
