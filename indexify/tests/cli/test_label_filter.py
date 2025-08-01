import contextlib
import time
import unittest
from typing import Dict, List

from tensorlake import Graph, LabelsFilter, tensorlake_function
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


@tensorlake_function(
    placement_constraints=LabelsFilter.all_of(environment="production")
)
def production_function() -> int:
    """Function that requires production environment."""
    return executor_pid()


@tensorlake_function(placement_constraints=LabelsFilter.all_of(gpu_type="nvidia"))
def gpu_function(_: int) -> int:
    """Function that requires nvidia GPU."""
    return executor_pid()


@tensorlake_function(
    placement_constraints=LabelsFilter.all_of(
        environment="production", gpu_type="nvidia"
    )
)
def production_gpu_function(_: int) -> int:
    """Function that requires both production environment and nvidia GPU."""
    return executor_pid()


@tensorlake_function(placement_constraints=LabelsFilter.all_of(region="us-west"))
def regional_function(_: int) -> int:
    """Function that requires us-west region."""
    return executor_pid()


@tensorlake_function()
def unrestricted_function(_: int) -> int:
    """Function with no placement constraints."""
    return executor_pid()


@tensorlake_function(
    placement_constraints=LabelsFilter.all_of(nonexistent_label="nonexistent_value")
)
def impossible_function() -> int:
    """Function that no executor will satisfy."""
    return executor_pid()


class TestLabelFilter(unittest.TestCase):
    def test_label_filter_routing(self):
        """Test that functions are routed only to executors with matching labels."""

        # Validate that:
        #
        # * Functions with placement constraints only run on executors
        #   with matching labels
        #
        # * Functions without constraints can run on any executor
        #
        # We do this statistically, by running multiple invocations
        # and verifying that the functions consistently land on the
        # correct executor; the test might pass if something's broken,
        # but it's unlikely.

        total_invokes = 5

        executors_pid: Dict[str, int] = {
            "dev_mode": -1,  # Existing dev mode executor (no label restrictions)
            "production": -1,  # Executor with environment=production label
            "gpu": -1,  # Executor with gpu_type=nvidia label
            "production_gpu": -1,  # Executor with both environment=production and gpu_type=nvidia
            "regional": -1,  # Executor with region=us-west label
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
                "name": "production",
                "labels": {"environment": "production"},
                "args": ["--monitoring-server-port", "7001"],
                "monitoring_port": 7001,
            },
            {
                "name": "gpu",
                "labels": {"gpu_type": "nvidia"},
                "args": ["--monitoring-server-port", "7002"],
                "monitoring_port": 7002,
            },
            {
                "name": "production_gpu",
                "labels": {"environment": "production", "gpu_type": "nvidia"},
                "args": ["--monitoring-server-port", "7003"],
                "monitoring_port": 7003,
            },
            {
                "name": "regional",
                "labels": {"region": "us-west"},
                "args": ["--monitoring-server-port", "7004"],
                "monitoring_port": 7004,
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
                description="Label filter routing test",
                start_node=production_function,
                version=version,
            )

            # Chain functions to test different placement constraints
            graph.add_edge(production_function, gpu_function)
            graph.add_edge(gpu_function, production_gpu_function)
            graph.add_edge(production_gpu_function, regional_function)
            graph.add_edge(regional_function, unrestricted_function)

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
                "production_function": [],
                "gpu_function": [],
                "production_gpu_function": [],
                "regional_function": [],
                "unrestricted_function": [],
            }

            for invocation_id in invocation_ids:
                # Test production_function: should only run on production or production_gpu executors
                output = wait_function_output(
                    graph, invocation_id, "production_function"
                )
                self.assertEqual(len(output), 1)
                func_executor_pid = output[0]
                function_executor_usage["production_function"].append(func_executor_pid)

                allowed_pids = [
                    executors_pid["production"],
                    executors_pid["production_gpu"],
                ]
                self.assertIn(
                    func_executor_pid,
                    allowed_pids,
                    f"production_function (PID {func_executor_pid}, {executors_name[func_executor_pid]}) should only run on production executor ({executors_pid['production']}) or production executor ({executors_pid['production_gpu']})",
                )

                # Test gpu_function: should only run on gpu or production_gpu executors
                output = wait_function_output(graph, invocation_id, "gpu_function")
                self.assertEqual(len(output), 1)
                func_executor_pid = output[0]
                function_executor_usage["gpu_function"].append(func_executor_pid)

                allowed_pids = [
                    executors_pid["gpu"],
                    executors_pid["production_gpu"],
                ]
                self.assertIn(
                    func_executor_pid,
                    allowed_pids,
                    f"gpu_function (PID {func_executor_pid}, {executors_name[func_executor_pid]}) should only run on gpu executor ({executors_pid['gpu']}) or production_gpu executor ({executors_pid['production_gpu']})",
                )

                # Test production_gpu_function: should only run on production_gpu executor
                output = wait_function_output(
                    graph, invocation_id, "production_gpu_function"
                )
                self.assertEqual(len(output), 1)
                func_executor_pid = output[0]
                function_executor_usage["production_gpu_function"].append(
                    func_executor_pid
                )

                allowed_pids = [
                    executors_pid["production_gpu"],
                ]
                self.assertIn(
                    func_executor_pid,
                    allowed_pids,
                    f"production_gpu_function (PID {func_executor_pid}, {executors_name[func_executor_pid]}) should only run on production_gpu executor ({executors_pid['production_gpu']})",
                )

                # Test regional_function: should only run on regional executor
                output = wait_function_output(graph, invocation_id, "regional_function")
                self.assertEqual(len(output), 1)
                func_executor_pid = output[0]
                function_executor_usage["regional_function"].append(func_executor_pid)

                allowed_pids = [executors_pid["regional"]]
                self.assertIn(
                    func_executor_pid,
                    allowed_pids,
                    f"regional_function (PID {func_executor_pid}, {executors_name[func_executor_pid]}) should only run on regional executor ({executors_pid['regional']})",
                )

                # Test unrestricted_function: can run on any executor
                output = wait_function_output(
                    graph, invocation_id, "unrestricted_function"
                )
                self.assertEqual(len(output), 1)
                func_executor_pid = output[0]
                function_executor_usage["unrestricted_function"].append(
                    func_executor_pid
                )

                all_executor_pids = list(executors_pid.values())
                self.assertIn(
                    func_executor_pid,
                    all_executor_pids,
                    f"unrestricted_function (PID {func_executor_pid}, {executors_name[func_executor_pid]}) ran on unknown executor. Known executors: {executors_pid}",
                )

    def test_no_matching_executor(self):
        """Test behavior when no executor matches the placement constraints."""
        # This test verifies that functions with placement constraints
        # that no executor can satisfy do not cause the system to
        # hang.

        graph_name = test_graph_name(self)
        version = str(time.time())

        # Deploy and run the impossible function.
        graph = Graph(
            name=graph_name + "_impossible",
            description="Test impossible placement constraints",
            start_node=impossible_function,
            version=version,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )

        invocation_id = graph.run(block_until_done=True)
        output = wait_function_output(graph, invocation_id, "impossible_function")

        self.assertEqual(len(output), 0)


if __name__ == "__main__":
    unittest.main()
