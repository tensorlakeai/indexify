import os
import subprocess
import time
import unittest
from typing import List, Optional

from tensorlake import Graph, tensorlake_function
from tensorlake.remote_graph import RemoteGraph
from testing import (
    ExecutorProcessContextManager,
    function_uri,
    test_graph_name,
    wait_executor_startup,
)

# There's a dev mode executor already running in the testing environment.
# It's used for all other tests that don't check the function allowlist.
# This existing Executor can run any function.
dev_mode_executor_pid: Optional[int] = None
# This Executor can only run function_a.
function_a_executor_pid: Optional[int] = None
# This Executor can only run function_b.
function_b_executor_pid: Optional[int] = None


def get_executor_pid() -> int:
    # Assuming Subprocess Function Executors are used in Open Source.
    return os.getppid()


@tensorlake_function()
def get_dev_mode_executor_pid() -> int:
    return get_executor_pid()


@tensorlake_function()
def function_a() -> str:
    global dev_mode_executor_pid
    global function_a_executor_pid
    global function_b_executor_pid

    current_executor_pid: int = get_executor_pid()
    allowed_executor_pids: List[int] = [function_a_executor_pid, dev_mode_executor_pid]
    if current_executor_pid not in allowed_executor_pids:
        raise Exception(
            f"function_a Executor PID {current_executor_pid} is not in the allowlist: {allowed_executor_pids}"
        )
    return "success"


@tensorlake_function()
def function_b(_: str) -> str:
    global dev_mode_executor_pid
    global function_a_executor_pid
    global function_b_executor_pid

    current_executor_pid: int = get_executor_pid()
    allowed_executor_pids: List[int] = [function_b_executor_pid, dev_mode_executor_pid]
    if current_executor_pid not in allowed_executor_pids:
        raise Exception(
            f"function_b Executor PID {current_executor_pid} is not in the allowlist: {allowed_executor_pids}"
        )
    return "success"


@tensorlake_function()
def function_dev(_: str) -> str:
    current_executor_pid: int = get_executor_pid()
    allowed_executor_pids: List[int] = [dev_mode_executor_pid]
    if current_executor_pid not in allowed_executor_pids:
        raise Exception(
            f"function_dev Executor PID {current_executor_pid} is not in the allowlist: {allowed_executor_pids}"
        )
    return "success"


class TestFunctionAllowlist(unittest.TestCase):
    def test_function_routing(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=get_dev_mode_executor_pid,
            version="1.0",
        )
        graph = RemoteGraph.deploy(graph)

        global dev_mode_executor_pid
        invocation_id = graph.run(block_until_done=True)
        output = graph.output(invocation_id, "get_dev_mode_executor_pid")
        self.assertEqual(len(output), 1)
        dev_mode_executor_pid = output[0]
        print(f"Found dev mode Executor PID: {dev_mode_executor_pid}")

        with ExecutorProcessContextManager(
            [
                "--function",
                function_uri(
                    "default",
                    "test_different_executors_run_different_functions",
                    "function_a",
                    "2.0",
                ),
                "--ports",
                "60000",
                "60001",
                "--api-port",
                "7001",
            ]
        ) as executor_a:
            executor_a: subprocess.Popen
            global function_a_executor_pid
            function_a_executor_pid = executor_a.pid
            print(f"Started Executor A with PID: {function_a_executor_pid}")
            wait_executor_startup(7001)

            with ExecutorProcessContextManager(
                [
                    "--function",
                    function_uri(
                        "default",
                        "test_different_executors_run_different_functions",
                        "function_b",
                        "2.0",
                    ),
                    "--ports",
                    "60001",
                    "60002",
                    "--api-port",
                    "7002",
                ]
            ) as executor_b:
                executor_b: subprocess.Popen
                global function_b_executor_pid
                function_b_executor_pid = executor_b.pid
                print(f"Started Executor B with PID: {function_b_executor_pid}")
                wait_executor_startup(7002)

                graph = Graph(
                    name=test_graph_name(self),
                    description="test",
                    start_node=function_a,
                    version="2.0",
                )
                graph.add_edge(function_a, function_b)
                graph.add_edge(function_b, function_dev)
                graph = RemoteGraph.deploy(graph)
                # As invocations might land on dev Executor, we need to run the graph multiple times
                # to ensure that we catch wrong routing to Executor A or B if it ever happens.
                for _ in range(10):
                    invocation_id = graph.run(block_until_done=True)
                    output = graph.output(invocation_id, "function_a")
                    self.assertEqual(len(output), 1)
                    self.assertEqual(output[0], "success")

                    output = graph.output(invocation_id, "function_b")
                    self.assertEqual(len(output), 1)
                    self.assertEqual(output[0], "success")

                    output = graph.output(invocation_id, "function_dev")
                    self.assertEqual(len(output), 1)
                    self.assertEqual(output[0], "success")


if __name__ == "__main__":
    unittest.main()
