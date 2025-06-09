import contextlib
import time
import unittest
from typing import Dict, List, Optional

from tensorlake import Graph, tensorlake_function
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
from tensorlake.remote_graph import RemoteGraph
from testing import (
    ExecutorProcessContextManager,
    executor_pid,
    function_uri,
    test_graph_name,
    wait_executor_startup,
    wait_function_output,
)


@tensorlake_function()
def get_dev_mode_executor_pid() -> int:
    return executor_pid()


@tensorlake_function()
def function_a() -> int:
    return executor_pid()


@tensorlake_function()
def function_b(_: int) -> int:
    return executor_pid()


@tensorlake_function()
def function_c(_: int) -> int:
    return executor_pid()


@tensorlake_function()
def function_dev(_: int) -> int:
    return executor_pid()


class TestFunctionAllowlist(unittest.TestCase):
    def test_tasks_routing_and_distribution(self):
        # This test verifies that function are routed only to the correct executors
        # and that the distribution of tasks is uniform across all executors.
        print(
            "Waiting for 30 seconds for Server to notice that any previously existing Executors exited."
        )
        time.sleep(30)

        # Executor PIDs for different function executors
        executors_pid: Dict[str, int] = {
            "dev_mode": -1,  # Existing dev mode executor that can run any function
            "function_a": -1,  # Executor that can only run function_a
            "function_b": -1,  # Executor that can only run function_b
            "function_c": -1,  # Executor that can only run function_c any version
        }

        graph_name = test_graph_name(self)
        version = str(time.time())

        # Get dev mode executor PID
        graph = Graph(
            name=graph_name + "_dev",
            description="test",
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
        print(f"Found dev mode Executor PID: {executors_pid['dev_mode']}")

        # Define executor configurations
        executor_configs = [
            {
                "name": "function_a",
                "args": [
                    "--function",
                    function_uri("default", graph_name, "function_a", version),
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
            graph = RemoteGraph.deploy(
                graph=graph, code_dir_path=graph_code_dir_path(__file__)
            )

            # Track tasks per executor
            tasks_per_executor_pid = {}
            # Run many invokes to get representative statistics
            total_invokes = 10
            total_tasks = total_invokes * 4

            invocation_ids = []
            for _ in range(total_invokes):
                invocation_ids.append(graph.run(block_until_done=True))

            print("Waiting for all invocations to finish...")
            for invocation_id in invocation_ids:
                # Check outputs for each function
                for func_name in [
                    "function_a",
                    "function_b",
                    "function_c",
                    "function_dev",
                ]:
                    if func_name == "function_a":
                        allowed_executor_pids: List[int] = [
                            executors_pid["function_a"],
                            executors_pid["dev_mode"],
                        ]
                    elif func_name == "function_b":
                        allowed_executor_pids: List[int] = [
                            executors_pid["function_b"],
                            executors_pid["dev_mode"],
                        ]
                    elif func_name == "function_c":
                        allowed_executor_pids: List[int] = [
                            executors_pid["function_c"],
                            executors_pid["dev_mode"],
                        ]
                    elif func_name == "function_dev":
                        allowed_executor_pids: List[int] = [executors_pid["dev_mode"]]

                    output = wait_function_output(graph, invocation_id, func_name)
                    self.assertEqual(len(output), 1)

                    func_executor_pid: int = output[0]
                    if func_executor_pid not in allowed_executor_pids:
                        raise Exception(
                            f"{func_name} Executor PID {func_executor_pid} is not in the allowlist: {allowed_executor_pids}"
                        )

                    tasks_per_executor_pid[func_executor_pid] = (
                        tasks_per_executor_pid.get(func_executor_pid, 0) + 1
                    )

            # Create mapping of executor PIDs to names for better reporting
            executor_pid_to_name = {
                executors_pid["dev_mode"]: "dev_mode",
                executors_pid["function_a"]: "function_a",
                executors_pid["function_b"]: "function_b",
                executors_pid["function_c"]: "function_c",
            }

            # Format the invocation counts
            tasks_per_executor_name = {
                executor_pid_to_name.get(
                    pid, f"unknown_executor_{pid}"
                ): tasks_per_executor_pid.get(pid, 0)
                for pid in executor_pid_to_name.keys()
            }

            print(f"Tasks distribution: {tasks_per_executor_name}")

            # Assert that all executors were used
            # self.assertEqual(
            #    len(tasks_per_executor_pid),
            #    4,
            #    f"Not all executors were used: {tasks_per_executor_name}",
            # )

            # For each function, calculate the expected distribution
            # All executors should run a uniform amount of functions.
            expected_counts = {
                "dev_mode": total_invokes,
                "function_a": total_invokes,
                "function_b": total_invokes,
                "function_c": total_invokes,
            }

            # Print a more detailed analysis of the distribution
            print("Distribution Analysis:")
            print(f"- Total tasks: {total_tasks}")
            for executor_name, count in tasks_per_executor_name.items():
                print(
                    f"- {executor_name}: {count} tasks ({count / (total_tasks) * 100:.1f}%)"
                )
                print(
                    f"  Expected: {expected_counts[executor_name]} ({expected_counts[executor_name] / (total_tasks) * 100:.1f}%)"
                )

            # Check that distributions are reasonably close to expected
            for executor_name, expected_count in expected_counts.items():
                actual_count = tasks_per_executor_name[executor_name]

                # Allow for 20% deviation from expected values
                lower_bound = expected_count * 0.8
                upper_bound = expected_count * 1.2

                # We have to rethink this test because the non allowlist executor can run
                # all the functions which are allow listed on other executors. The distribution
                # will be skewed towards the non allowlist executor.

                # self.assertTrue(
                #    lower_bound <= actual_count <= upper_bound,
                #    f"Executor {executor_name} invocation count ({actual_count}) "
                #    f"is not within 20% of expected count ({expected_count}). "
                #    f"Distribution: {tasks_per_executor_name}",
                # )


if __name__ == "__main__":
    unittest.main()
