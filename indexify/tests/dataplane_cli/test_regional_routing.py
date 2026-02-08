import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import contextlib
import unittest
from typing import Dict, List

from tensorlake.applications import (
    Request,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications
from cli.testing import executor_pid
from dataplane_cli.testing import (
    DataplaneProcessContextManager,
    wait_dataplane_startup,
)


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


class TestRegionalRoutingDataplane(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy_applications(__file__)

    def test_regional_routing(self):
        """Test that functions are routed to executors in the correct regions."""

        # Validate that functions with regional constraints only run on
        # executors in the matching region. This tests the new catalog-based
        # executor labeling system where executors have 'sku' and 'region' labels
        # that must match entries in the executor catalog.
        #
        # We do this statistically, by running multiple invocations
        # and verifying that the functions consistently land on the
        # correct regional executor; the test might pass if something's broken,
        # but it's unlikely.

        NUM_REQUESTS_PER_REGION = 5

        executor_to_pid: Dict[str, int] = {
            "dev_mode": -1,  # Existing dev mode executor (no label restrictions)
            "us_east_1": -1,  # Executor with sku=cpu-s, region=us-east-1
            "us_west_2": -1,  # Executor with sku=gpu-xl, region=us-west-2
            "eu_west_1": -1,  # Executor with sku=gpu-xxl, region=eu-west-1
        }

        get_dev_mode_executor_pid_request: Request = run_remote_application(
            get_dev_mode_executor_pid, 0
        )
        executor_to_pid["dev_mode"] = get_dev_mode_executor_pid_request.output()
        print(f"Dev mode executor PID: {executor_to_pid['dev_mode']}")

        executor_configs: List[dict] = [
            {
                "name": "us_east_1",
                "labels": {"sku": "cpu-s", "region": "us-east-1"},
                "config_overrides": {
                    "http_proxy": {"port": 7001}
                },
                "monitoring_port": 7001,
            },
            {
                "name": "us_west_2",
                "labels": {"sku": "gpu-xl", "region": "us-west-2"},
                "config_overrides": {
                    "http_proxy": {"port": 7002}
                },
                "monitoring_port": 7002,
            },
            {
                "name": "eu_west_1",
                "labels": {"sku": "gpu-xxl", "region": "eu-west-1"},
                "config_overrides": {
                    "http_proxy": {"port": 7003}
                },
                "monitoring_port": 7003,
            },
        ]

        executor_cms = [
            DataplaneProcessContextManager(
                config_overrides=config["config_overrides"],
                keep_std_outputs=True,
                labels=config["labels"],
            )
            for config in executor_configs
        ]

        with contextlib.ExitStack() as stack:
            pid_to_executor_name: Dict[int, str] = {
                executor_to_pid["dev_mode"]: "dev_mode"
            }
            for i, (cm, config) in enumerate(zip(executor_cms, executor_configs)):
                proc = stack.enter_context(cm)
                executor_to_pid[config["name"]] = proc.pid
                print(
                    f"Started {config['name']} executor (PID: {proc.pid}) with labels: {config['labels']}"
                )
                pid_to_executor_name[proc.pid] = config["name"]

            for config in executor_configs:
                wait_dataplane_startup(config["monitoring_port"])
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


if __name__ == "__main__":
    unittest.main()
