import threading
import time
import unittest

from tensorlake import Graph, RemoteGraph, tensorlake_function
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
from testing import test_graph_name


@tensorlake_function()
def sleep_a(secs: int) -> str:
    time.sleep(secs)
    return "success"


@tensorlake_function()
def sleep_b(secs: int) -> str:
    time.sleep(secs)
    return "success"


def invoke_sleep_graph(graph_name, func_name, func_arg_secs: int):
    graph = RemoteGraph.by_name(graph_name)
    invocation_id = graph.run(
        block_until_done=True,
        secs=func_arg_secs,
    )
    # Run in a new process because this call blocks and with threads
    # we won't be able to run it with a real concurrency.
    output = graph.output(invocation_id, func_name)
    if output != ["success"]:
        raise Exception(f"Expected output to be ['success'], got {output}")


class TestRemoteGraphFunctionConcurrency(unittest.TestCase):
    def test_two_same_functions_run_with_concurrency_of_one(self):
        # This test verifies that two invocations of the same function run sequentially
        # because a single Executor can have only one Function Executor per function
        # version and because each Function Executor can only run a single task concurrently.
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=sleep_a,
        )
        graph = RemoteGraph.deploy(graph, code_dir_path=graph_code_dir_path(__file__))

        # Pre-warm Executor so Executor delays in the next invokes are very low.
        invoke_sleep_graph(
            graph_name=test_graph_name(self), func_name="sleep_a", func_arg_secs=0.01
        )

        threads = [
            threading.Thread(
                target=invoke_sleep_graph,
                kwargs={
                    "graph_name": test_graph_name(self),
                    "func_name": "sleep_a",
                    "func_arg_secs": 0.51,
                },
            ),
            threading.Thread(
                target=invoke_sleep_graph,
                kwargs={
                    "graph_name": test_graph_name(self),
                    "func_name": "sleep_a",
                    "func_arg_secs": 0.51,
                },
            ),
        ]

        for thread in threads:
            thread.start()

        start_time = time.time()
        for thread in threads:
            thread.join()

        end_time = time.time()
        duration = end_time - start_time
        self.assertGreaterEqual(
            duration,
            1.0,
            "The two invocations of the same function should run sequentially",
        )

    def test_two_different_functions_run_with_concurrency_of_two(self):
        # This test verifies that two invocations of different functions run concurrently
        # because a single Executor can have a Function Executor for each different function.
        graph_a_name = test_graph_name(self) + "_a"
        graph_a = Graph(
            name=graph_a_name,
            description="test",
            start_node=sleep_a,
        )
        graph_a = RemoteGraph.deploy(
            graph_a, code_dir_path=graph_code_dir_path(__file__)
        )

        graph_b_name = test_graph_name(self) + "_b"
        graph_b = Graph(
            name=graph_b_name,
            description="test",
            start_node=sleep_b,
        )
        graph_b = RemoteGraph.deploy(
            graph_b, code_dir_path=graph_code_dir_path(__file__)
        )

        # Pre-warm Executor so Executor delays in the next invokes are very low.
        invoke_sleep_graph(
            graph_name=graph_a_name, func_name="sleep_a", func_arg_secs=0.01
        )
        invoke_sleep_graph(
            graph_name=graph_b_name, func_name="sleep_b", func_arg_secs=0.01
        )

        threads = [
            threading.Thread(
                target=invoke_sleep_graph,
                kwargs={
                    "graph_name": graph_a_name,
                    "func_name": "sleep_a",
                    "func_arg_secs": 0.51,
                },
            ),
            threading.Thread(
                target=invoke_sleep_graph,
                kwargs={
                    "graph_name": graph_b_name,
                    "func_name": "sleep_b",
                    "func_arg_secs": 0.51,
                },
            ),
        ]

        for thread in threads:
            thread.start()

        start_time = time.time()
        for thread in threads:
            thread.join()

        end_time = time.time()
        duration = end_time - start_time
        self.assertLessEqual(
            duration,
            1.0,
            "The two invocations of different functions should run concurrently",
        )


if __name__ == "__main__":
    unittest.main()
