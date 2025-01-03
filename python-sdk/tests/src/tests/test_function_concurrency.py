import threading
import time
import unittest

from indexify import Graph, indexify_function
from tests.testing import remote_or_local_graph, test_graph_name


@indexify_function()
def sleep_a(secs: int) -> str:
    time.sleep(secs)
    return "success"


@indexify_function()
def sleep_b(secs: int) -> str:
    time.sleep(secs)
    return "success"


class TestRemoteGraphFunctionConcurrency(unittest.TestCase):
    def test_two_same_functions_run_with_concurrency_of_one(self):
        is_remote = True
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=sleep_a,
        )
        graph = remote_or_local_graph(graph, is_remote)

        def invoke_sleep_a(secs: int):
            invocation_id = graph.run(
                block_until_done=True,
                secs=secs,
            )
            output = graph.output(invocation_id, "sleep_a")
            self.assertEqual(output, ["success"])

        # Pre-warm Executor so Executor delays in the next invokes are very low.
        invoke_sleep_a(0.01)

        threads = [
            threading.Thread(target=invoke_sleep_a, args=(0.51,)),
            threading.Thread(target=invoke_sleep_a, args=(0.51,)),
        ]

        start_time = time.time()
        for thread in threads:
            thread.start()
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
        is_remote = True
        graph_a = Graph(
            name=test_graph_name(self) + "_a",
            description="test",
            start_node=sleep_a,
        )
        graph_a = remote_or_local_graph(graph_a, is_remote)

        def invoke_sleep_a(secs: int):
            invocation_id = graph_a.run(
                block_until_done=True,
                secs=secs,
            )
            output = graph_a.output(invocation_id, "sleep_a")
            self.assertEqual(output, ["success"])

        graph_b = Graph(
            name=test_graph_name(self) + "_b",
            description="test",
            start_node=sleep_b,
        )
        graph_b = remote_or_local_graph(graph_b, is_remote)

        def invoke_sleep_b(secs: int):
            invocation_id = graph_b.run(
                block_until_done=True,
                secs=secs,
            )
            output = graph_b.output(invocation_id, "sleep_b")
            self.assertEqual(output, ["success"])

        # Pre-warm Executor so Executor delays in the next invokes are very low.
        invoke_sleep_a(0.01)
        invoke_sleep_b(0.01)

        threads = [
            threading.Thread(target=invoke_sleep_a, args=(0.51,)),
            threading.Thread(target=invoke_sleep_b, args=(0.51,)),
        ]

        start_time = time.time()
        for thread in threads:
            thread.start()
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
