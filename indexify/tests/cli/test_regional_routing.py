import contextlib
import time
import unittest
from typing import Dict, List

from tensorlake import Graph, tensorlake_function
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
from tensorlake.functions_sdk.remote_graph import RemoteGraph
from testing import (
    ExecutorProcessContextManager,
    executor_pid,
    test_graph_name,
    wait_executor_startup,
    wait_function_output,
)


@tensorlake_function()
def get_dev_mode_executor_pid() -> int:
    """Returns the PID of the executor running this function."""
    return executor_pid()


@tensorlake_function(region="us-east-1")
def regional_function_east() -> int:
    """Function that requires us-east-1 region."""
    return executor_pid()


@tensorlake_function(region="us-west-2")
def regional_function_west(_: int) -> int:
    """Function that requires us-west-2 region."""
    return executor_pid()


@tensorlake_function(region="eu-west-1")
def regional_function_eu(_: int) -> int:
    """Function that requires eu-west-1 region."""
    return executor_pid()


@tensorlake_function(region="us-east-1")
def regional_function_east_2(_: int) -> int:
    """Function that requires us-east-1 region."""
    return executor_pid()


@tensorlake_function(region="us-west-2")
def regional_function_west_2(_: int) -> int:
    """Function that requires us-west-2 region."""
    return executor_pid()


class TestRegionalRouting(unittest.TestCase):
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

        total_invokes = 5

        executors_pid: Dict[str, int] = {
            "dev_mode": -1,  # Existing dev mode executor (no label restrictions)
            "us_east_1": -1,  # Executor with sku=cpu-s, region=us-east-1
            "us_west_2": -1,  # Executor with sku=gpu-xl, region=us-west-2
            "eu_west_1": -1,  # Executor with sku=gpu-xxl, region=eu-west-1
        }

        graph_name = test_graph_name(self)
        version = str(time.time())

        print("Getting dev mode executor PID...")
        graph = Graph(
            name=graph_name + "_dev",
            description="Get dev mode executor PID",
            start_node=get_dev_mode_executor_pid,
            version=version,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )
        invocation_id = graph.run(block_until_done=True)
        output = graph.output(invocation_id, "get_dev_mode_executor_pid")
        self.assertEqual(len(output), 1)
        executors_pid["dev_mode"] = output[0]
        print(f"Dev mode executor PID: {executors_pid['dev_mode']}")

        executor_configs = [
            {
                "name": "us_east_1",
                "labels": {"sku": "cpu-s", "region": "us-east-1"},
                "args": ["--monitoring-server-port", "7001"],
                "monitoring_port": 7001,
            },
            {
                "name": "us_west_2",
                "labels": {"sku": "gpu-xl", "region": "us-west-2"},
                "args": ["--monitoring-server-port", "7002"],
                "monitoring_port": 7002,
            },
            {
                "name": "eu_west_1",
                "labels": {"sku": "gpu-xxl", "region": "eu-west-1"},
                "args": ["--monitoring-server-port", "7003"],
                "monitoring_port": 7003,
            },
        ]

        executor_cms = [
            ExecutorProcessContextManager(
                config["args"],
                keep_std_outputs=False,
                labels=config["labels"],
            )
            for config in executor_configs
        ]

        with contextlib.ExitStack() as stack:
            executors_name = {executors_pid["dev_mode"]: "dev_mode"}
            for i, (cm, config) in enumerate(zip(executor_cms, executor_configs)):
                proc = stack.enter_context(cm)
                executors_pid[config["name"]] = proc.pid
                print(
                    f"Started {config['name']} executor (PID: {proc.pid}) with labels: {config['labels']}"
                )
                executors_name[proc.pid] = config["name"]

            for config in executor_configs:
                wait_executor_startup(config["monitoring_port"])
                print(f"Executor {config['name']} is ready")

            graph = Graph(
                name=graph_name,
                description="Regional label filter routing test",
                start_node=regional_function_east,
                version=version,
            )

            # Chain functions to test different regional constraints
            graph.add_edge(regional_function_east, regional_function_west)
            graph.add_edge(regional_function_west, regional_function_eu)
            graph.add_edge(regional_function_eu, regional_function_east_2)
            graph.add_edge(regional_function_east_2, regional_function_west_2)

            graph = RemoteGraph.deploy(
                graph=graph, code_dir_path=graph_code_dir_path(__file__)
            )

            # Run multiple invocations to test label filtering
            invocation_ids = []

            print(f"Running {total_invokes} invocations...")
            for _ in range(total_invokes):
                invocation_ids.append(graph.run(block_until_done=True))

            # Track which executors ran each function
            function_executor_usage: Dict[str, List[int]] = {
                "regional_function_east": [],
                "regional_function_west": [],
                "regional_function_eu": [],
                "regional_function_east_2": [],
                "regional_function_west_2": [],
            }

            for invocation_id in invocation_ids:
                # Test regional_function_east: should only run on us-east-1 executor
                output = wait_function_output(
                    graph, invocation_id, "regional_function_east"
                )
                self.assertEqual(len(output), 1)
                func_executor_pid = output[0]
                function_executor_usage["regional_function_east"].append(
                    func_executor_pid
                )

                allowed_pids = [executors_pid["us_east_1"]]
                self.assertIn(
                    func_executor_pid,
                    allowed_pids,
                    f"regional_function_east (PID {func_executor_pid}, {executors_name[func_executor_pid]}) should only run on us-east-1 executor ({executors_pid['us_east_1']})",
                )

                # Test regional_function_west: should only run on us-west-2 executor
                output = wait_function_output(
                    graph, invocation_id, "regional_function_west"
                )
                self.assertEqual(len(output), 1)
                func_executor_pid = output[0]
                function_executor_usage["regional_function_west"].append(
                    func_executor_pid
                )

                allowed_pids = [executors_pid["us_west_2"]]
                self.assertIn(
                    func_executor_pid,
                    allowed_pids,
                    f"regional_function_west (PID {func_executor_pid}, {executors_name[func_executor_pid]}) should only run on us-west-2 executor ({executors_pid['us_west_2']})",
                )

                # Test regional_function_eu: should only run on eu-west-1 executor
                output = wait_function_output(
                    graph, invocation_id, "regional_function_eu"
                )
                self.assertEqual(len(output), 1)
                func_executor_pid = output[0]
                function_executor_usage["regional_function_eu"].append(
                    func_executor_pid
                )

                allowed_pids = [executors_pid["eu_west_1"]]
                self.assertIn(
                    func_executor_pid,
                    allowed_pids,
                    f"regional_function_eu (PID {func_executor_pid}, {executors_name[func_executor_pid]}) should only run on eu-west-1 executor ({executors_pid['eu_west_1']})",
                )

                # Test regional_function_east_2: should only run on us-east-1 executor
                output = wait_function_output(
                    graph, invocation_id, "regional_function_east_2"
                )
                self.assertEqual(len(output), 1)
                func_executor_pid = output[0]
                function_executor_usage["regional_function_east_2"].append(
                    func_executor_pid
                )

                allowed_pids = [executors_pid["us_east_1"]]
                self.assertIn(
                    func_executor_pid,
                    allowed_pids,
                    f"regional_function_east_2 (PID {func_executor_pid}, {executors_name[func_executor_pid]}) should only run on us-east-1 executor ({executors_pid['us_east_1']})",
                )

                # Test regional_function_west_2: should only run on us-west-2 executor
                output = wait_function_output(
                    graph, invocation_id, "regional_function_west_2"
                )
                self.assertEqual(len(output), 1)
                func_executor_pid = output[0]
                function_executor_usage["regional_function_west_2"].append(
                    func_executor_pid
                )

                allowed_pids = [executors_pid["us_west_2"]]
                self.assertIn(
                    func_executor_pid,
                    allowed_pids,
                    f"regional_function_west_2 (PID {func_executor_pid}, {executors_name[func_executor_pid]}) should only run on us-west-2 executor ({executors_pid['us_west_2']})",
                )


if __name__ == "__main__":
    unittest.main()
