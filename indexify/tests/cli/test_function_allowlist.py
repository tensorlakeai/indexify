import subprocess
import time
import unittest
from typing import List, Optional

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

# There's a dev mode executor already running in the testing environment.
# It's used for all other tests that don't check the function allowlist.
# This existing Executor can run any function.
dev_mode_executor_pid: Optional[int] = None
# This Executor can only run function_a.
function_a_executor_pid: Optional[int] = None
# This Executor can only run function_b.
function_b_executor_pid: Optional[int] = None
# This Executor can only run function_c any version.
function_c_executor_pid: Optional[int] = None


@tensorlake_function()
def get_dev_mode_executor_pid() -> int:
    return executor_pid()


@tensorlake_function()
def function_a() -> int:
    global dev_mode_executor_pid
    global function_a_executor_pid
    global function_b_executor_pid

    current_executor_pid: int = executor_pid()
    allowed_executor_pids: List[int] = [function_a_executor_pid, dev_mode_executor_pid]
    if current_executor_pid not in allowed_executor_pids:
        raise Exception(
            f"function_a Executor PID {current_executor_pid} is not in the allowlist: {allowed_executor_pids}"
        )
    return current_executor_pid


@tensorlake_function()
def function_b(_: str) -> int:
    global function_b_executor_pid

    current_executor_pid: int = executor_pid()
    allowed_executor_pids: List[int] = [function_b_executor_pid, dev_mode_executor_pid]
    if current_executor_pid not in allowed_executor_pids:
        raise Exception(
            f"function_b Executor PID {current_executor_pid} is not in the allowlist: {allowed_executor_pids}"
        )
    return current_executor_pid


@tensorlake_function()
def function_c(_: str) -> int:
    global function_c_executor_pid

    current_executor_pid: int = executor_pid()
    allowed_executor_pids: List[int] = [function_c_executor_pid, dev_mode_executor_pid]
    if current_executor_pid not in allowed_executor_pids:
        raise Exception(
            f"function_c Executor PID {current_executor_pid} is not in the allowlist: {allowed_executor_pids}"
        )
    return current_executor_pid


@tensorlake_function()
def function_dev(_: str) -> int:
    current_executor_pid: int = executor_pid()
    allowed_executor_pids: List[int] = [dev_mode_executor_pid]
    if current_executor_pid not in allowed_executor_pids:
        raise Exception(
            f"function_dev Executor PID {current_executor_pid} is not in the allowlist: {allowed_executor_pids}"
        )
    return current_executor_pid


class TestFunctionAllowlist(unittest.TestCase):
    def test_function_routing(self):
        graph_name = test_graph_name(self)
        version = str(time.time())
        graph = Graph(
            name=graph_name + "_dev",
            description="test",
            start_node=get_dev_mode_executor_pid,
            version=version,
        )
        graph = RemoteGraph.deploy(graph, additional_modules=[testing])

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
                    graph_name,
                    "function_a",
                    version,
                ),
                "--ports",
                "60000",
                "60001",
                "--monitoring-server-port",
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
                        graph_name,
                        "function_b",
                        version,
                    ),
                    "--ports",
                    "60001",
                    "60002",
                    "--monitoring-server-port",
                    "7002",
                ]
            ) as executor_b:
                executor_b: subprocess.Popen
                global function_b_executor_pid
                function_b_executor_pid = executor_b.pid
                print(f"Started Executor B with PID: {function_b_executor_pid}")
                wait_executor_startup(7002)

                with ExecutorProcessContextManager(
                    [
                        "--function",
                        function_uri(
                            "default",
                            graph_name,
                            "function_c",
                        ),
                        "--ports",
                        "60003",
                        "60004",
                        "--monitoring-server-port",
                        "7003",
                    ]
                ) as executor_c:
                    executor_c: subprocess.Popen
                    global function_c_executor_pid
                    function_c_executor_pid = executor_c.pid
                    print(f"Started Executor C with PID: {function_c_executor_pid}")
                    wait_executor_startup(7002)

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

                    invocations_per_pid = {}

                    # As invocations might land on dev Executor, we need to run the graph multiple times
                    # to ensure that we land on all executors.
                    for _ in range(200):
                        invocation_id = graph.run(block_until_done=True)
                        output = graph.output(invocation_id, "function_a")
                        self.assertEqual(len(output), 1)
                        invocations_per_pid[output[0]] = (
                            invocations_per_pid.get(output[0], 0) + 1
                        )

                        output = graph.output(invocation_id, "function_b")
                        self.assertEqual(len(output), 1)
                        invocations_per_pid[output[0]] = (
                            invocations_per_pid.get(output[0], 0) + 1
                        )
                        output = graph.output(invocation_id, "function_c")
                        self.assertEqual(len(output), 1)
                        invocations_per_pid[output[0]] = (
                            invocations_per_pid.get(output[0], 0) + 1
                        )

                        output = graph.output(invocation_id, "function_dev")
                        self.assertEqual(len(output), 1)
                        invocations_per_pid[output[0]] = (
                            invocations_per_pid.get(output[0], 0) + 1
                        )

                        # Check that all invocations have been routed to the correct executors
                        # at least once.
                        if len(invocations_per_pid) == 4:
                            break

                executor_names = {
                    dev_mode_executor_pid: "executor_dev",
                    function_a_executor_pid: "executor_a",
                    function_b_executor_pid: "executor_b",
                    function_c_executor_pid: "executor_c",
                }
                formatted_invocations = {
                    executor_names.get(
                        pid, f"unknown_executor_{pid}"
                    ): invocations_per_pid.get(pid, 0)
                    for pid in executor_names.keys()
                }
                assert (
                    len(invocations_per_pid) == 4
                ), f"Not all executors were used: {formatted_invocations}"


if __name__ == "__main__":
    unittest.main()
