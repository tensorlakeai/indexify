import time
import unittest

from tensorlake import Graph, tensorlake_function
from tensorlake.remote_graph import RemoteGraph


@tensorlake_function()
def get_start_time(x: int) -> str:
    return str(time.time())


class TestInvokeDurations(unittest.TestCase):
    def test_cold_start_duration_is_less_than_30_sec(self):
        graph = Graph(
            name="test_cold_start_duration_is_less_than_30_sec",
            description="test",
            start_node=get_start_time,
        )
        graph = RemoteGraph.deploy(graph)

        invoke_start_time = time.time()
        invocation_id = graph.run(block_until_done=True, x=1)
        output = graph.output(invocation_id, "get_start_time")
        self.assertEqual(len(output), 1)

        func_start_time = float(output[0])
        cold_start_duration = func_start_time - invoke_start_time
        print(f"cold_start_duration: {cold_start_duration} seconds")
        # Set the threshold to large 30 seconds to only catch large
        # regressions in server-sse-stream-stable branch because otherwise
        # we are not going to fix them.
        self.assertLess(cold_start_duration, 30)

    def test_warm_start_duration_is_less_than_hundred_ms(self):
        graph = Graph(
            name="test_warm_start_duration_is_less_than_hundred_ms",
            description="test",
            start_node=get_start_time,
        )
        graph = RemoteGraph.deploy(graph)

        # Cold start first.
        invocation_id = graph.run(block_until_done=True, x=1)
        output = graph.output(invocation_id, "get_start_time")
        self.assertEqual(len(output), 1)

        # Measure warm start duration.
        invoke_start_time = time.time()
        invocation_id = graph.run(block_until_done=True, x=2)
        output = graph.output(invocation_id, "get_start_time")
        self.assertEqual(len(output), 1)

        func_start_time = float(output[0])
        warm_start_duration = func_start_time - invoke_start_time
        print(f"warm_start_duration: {warm_start_duration} seconds")
        # The current duration we see in tests is about 20 ms.
        #
        # We give a large 100 ms headroom to prevent this test getting flaky
        # while still notifiying us if the warm start duration regresses
        # significantly.
        self.assertLess(warm_start_duration, 0.1)


if __name__ == "__main__":
    unittest.main()
