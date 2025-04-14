import contextlib
import subprocess
import time
import unittest
from typing import Dict, List, Optional

import testing
from tensorlake import Graph, tensorlake_function
from tensorlake.remote_graph import RemoteGraph
from testing import (
    ExecutorProcessContextManager,
    executor_pid,
    function_uri,
    test_graph_name,
    wait_executor_startup,
)

# Executor PIDs for different function executors
executors_pid: Dict[str, Optional[int]] = {
    "dev_mode": None,  # Existing dev mode executor that can run any function
    "function_a": None,  # Executor that can only run function_a
    "function_b": None,  # Executor that can only run function_b
    "function_c": None,  # Executor that can only run function_c any version
}


@tensorlake_function()
def get_dev_mode_executor_pid() -> int:
    return executor_pid()


@tensorlake_function()
def function_a() -> int:
    current_executor_pid: int = executor_pid()
    allowed_executor_pids: List[int] = [
        executors_pid["function_a"],
        executors_pid["dev_mode"],
    ]
    if current_executor_pid not in allowed_executor_pids:
        raise Exception(
            f"function_a Executor PID {current_executor_pid} is not in the allowlist: {allowed_executor_pids}"
        )
    return current_executor_pid


@tensorlake_function()
def function_b(_: str) -> int:
    current_executor_pid: int = executor_pid()
    allowed_executor_pids: List[int] = [
        executors_pid["function_b"],
        executors_pid["dev_mode"],
    ]
    if current_executor_pid not in allowed_executor_pids:
        raise Exception(
            f"function_b Executor PID {current_executor_pid} is not in the allowlist: {allowed_executor_pids}"
        )
    return current_executor_pid


@tensorlake_function()
def function_c(_: str) -> int:
    current_executor_pid: int = executor_pid()
    allowed_executor_pids: List[int] = [
        executors_pid["function_c"],
        executors_pid["dev_mode"],
    ]
    if current_executor_pid not in allowed_executor_pids:
        raise Exception(
            f"function_c Executor PID {current_executor_pid} is not in the allowlist: {allowed_executor_pids}"
        )
    return current_executor_pid


@tensorlake_function()
def function_dev(_: str) -> int:
    current_executor_pid: int = executor_pid()
    allowed_executor_pids: List[int] = [executors_pid["dev_mode"]]
    if current_executor_pid not in allowed_executor_pids:
        raise Exception(
            f"function_dev Executor PID {current_executor_pid} is not in the allowlist: {allowed_executor_pids}"
        )
    return current_executor_pid


class TestFunctionAllowlist(unittest.TestCase):
    def test_function_routing(self):
        print(
            "Waiting for 30 seconds for Server to notice that any previously existing Executors exited."
        )
        time.sleep(30)

        graph_name = test_graph_name(self)
        version = str(time.time())

        # Get dev mode executor PID
        graph = Graph(
            name=graph_name + "_dev",
            description="test",
            start_node=get_dev_mode_executor_pid,
            version=version,
        )
        graph = RemoteGraph.deploy(graph, additional_modules=[testing])
        invocation_id = graph.run(block_until_done=True)
        output = graph.output(invocation_id, "get_dev_mode_executor_pid")
        self.assertEqual(len(output), 1)
        executors_pid["dev_mode"] = output[0]
        print(f"Found dev mode Executor PID: {executors_pid['dev_mode']}")

        # Define executor configurations
        executor_configs = [
            {
                "name": "function_a",
                "args": [
                    "--function",
                    function_uri("default", graph_name, "function_a", version),
                    "--ports",
                    "60000",
                    "60001",
                    "--monitoring-server-port",
                    "7001",
                ],
                "monitoring_port": 7001,
            },
            {
                "name": "function_b",
                "args": [
                    "--function",
                    function_uri("default", graph_name, "function_b", version),
                    "--ports",
                    "60001",
                    "60002",
                    "--monitoring-server-port",
                    "7002",
                ],
                "monitoring_port": 7002,
            },
            {
                "name": "function_c",
                "args": [
                    "--function",
                    function_uri("default", graph_name, "function_c"),
                    "--ports",
                    "60003",
                    "60004",
                    "--monitoring-server-port",
                    "7003",
                ],
                "monitoring_port": 7003,
            },
        ]

        # Create context managers for each executor
        executor_cms = [
            ExecutorProcessContextManager(
                config["args"],
                keep_std_outputs=False,
            )
            for config in executor_configs
        ]

        # Use contextlib.ExitStack to manage multiple context managers
        with contextlib.ExitStack() as stack:
            # First enter all executor context managers to start them
            for i, (cm, config) in enumerate(zip(executor_cms, executor_configs)):
                proc = stack.enter_context(cm)
                # Store the PID for this executor
                executors_pid[config["name"]] = proc.pid
                print(f"Started Executor {config['name']} with PID: {proc.pid}")

            # Now wait for all executors to be ready
            for config in executor_configs:
                wait_executor_startup(config["monitoring_port"])
                print(f"Executor {config['name']} is ready")

            # Create and deploy the main graph
            graph = Graph(
                name=graph_name,
                description="test",
                start_node=function_a,
                version=version,
            )
            graph.add_edge(function_a, function_b)
            graph.add_edge(function_b, function_c)
            graph.add_edge(function_c, function_dev)
            graph = RemoteGraph.deploy(graph, additional_modules=[testing])

            # Track invocations per executor
            invocations_per_pid = {}

            # Define number of total runs
            num_runs = 400

            # Run the graph multiple times to ensure we land on all executors
            for _ in range(num_runs):
                invocation_id = graph.run(block_until_done=True)

                # Check outputs for each function
                for func_name in [
                    "function_a",
                    "function_b",
                    "function_c",
                    "function_dev",
                ]:
                    output = graph.output(invocation_id, func_name)
                    self.assertEqual(len(output), 1)
                    invocations_per_pid[output[0]] = (
                        invocations_per_pid.get(output[0], 0) + 1
                    )

            # Create mapping of executor PIDs to names for better reporting
            executor_names = {
                executors_pid["dev_mode"]: "executor_dev",
                executors_pid["function_a"]: "executor_a",
                executors_pid["function_b"]: "executor_b",
                executors_pid["function_c"]: "executor_c",
            }

            # Format the invocation counts
            formatted_invocations = {
                executor_names.get(
                    pid, f"unknown_executor_{pid}"
                ): invocations_per_pid.get(pid, 0)
                for pid in executor_names.keys()
            }

            print(f"Invocation distribution: {formatted_invocations}")

            # Assert that all executors were used
            self.assertEqual(
                len(invocations_per_pid),
                4,
                f"Not all executors were used: {formatted_invocations}",
            )

            # Assert that all executors were used
            self.assertEqual(
                len(invocations_per_pid),
                4,
                f"Not all executors were used: {formatted_invocations}",
            )

            # For each function, calculate the expected distribution
            # The dev executor should handle:
            # - All function_dev invocations
            # - Roughly 50% of function_a, function_b, and function_c invocations
            expected_counts = {
                executor_names[executors_pid["dev_mode"]]: num_runs
                * (1 + 0.5 * 3),  # 100% of function_dev + ~50% of others
                executor_names[executors_pid["function_a"]]: num_runs
                * 0.5,  # ~50% of function_a
                executor_names[executors_pid["function_b"]]: num_runs
                * 0.5,  # ~50% of function_b
                executor_names[executors_pid["function_c"]]: num_runs
                * 0.5,  # ~50% of function_c
            }

            # Print a more detailed analysis of the distribution
            print("Distribution Analysis:")
            print(f"- Total invocations: {num_runs * 4}")
            for executor_name, count in formatted_invocations.items():
                print(
                    f"- {executor_name}: {count} invocations ({count / (num_runs * 4) * 100:.1f}%)"
                )
                print(
                    f"  Expected: {expected_counts[executor_name]} ({expected_counts[executor_name] / (num_runs * 4) * 100:.1f}%)"
                )

            # Check that distributions are reasonably close to expected
            for executor_name, expected_count in expected_counts.items():
                actual_count = formatted_invocations[executor_name]

                # Allow for 20% deviation from expected values
                lower_bound = expected_count * 0.8
                upper_bound = expected_count * 1.2

                self.assertTrue(
                    lower_bound <= actual_count <= upper_bound,
                    f"Executor {executor_name} invocation count ({actual_count}) "
                    f"is not within 20% of expected count ({expected_count}). "
                    f"Distribution: {formatted_invocations}",
                )


if __name__ == "__main__":
    unittest.main()
