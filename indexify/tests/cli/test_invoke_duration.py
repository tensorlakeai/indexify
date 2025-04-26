import time
import unittest

from tensorlake import Graph, TensorlakeCompute, tensorlake_function
from tensorlake.remote_graph import RemoteGraph
from testing import test_graph_name


class ColdStartMeasurementFunction(TensorlakeCompute):
    name = "ColdStartMeasurementFunction"

    def __init__(self):
        super().__init__()
        # Records actual time when the function was initialized.
        # This allows to not measure the latency of Server learning that Function Executor was created.
        self._init_time: float = time.time()

    def run(self, x: int) -> str:
        return str(self._init_time)


@tensorlake_function()
def get_start_time(x: int) -> str:
    return str(time.time())


class TestInvokeDurations(unittest.TestCase):
    def test_cold_start_duration_is_less_than_ten_sec(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=ColdStartMeasurementFunction,
        )
        graph = RemoteGraph.deploy(graph)

        invoke_start_time = time.time()
        invocation_id = graph.run(block_until_done=True, x=1)
        output = graph.output(invocation_id, ColdStartMeasurementFunction.name)
        self.assertEqual(len(output), 1)

        func_init_time = float(output[0])
        cold_start_duration = func_init_time - invoke_start_time
        print(f"cold_start_duration: {cold_start_duration} seconds")
        # The current duration we see in tests is about 3 seconds
        # with p100 of 5 secs.
        #
        # We give a large headroom to prevent this test getting flaky
        # while still notifiying us if the cold start duration regresses
        # significantly.
        self.assertLess(cold_start_duration, 10)

    def test_warm_start_duration_is_less_than_hundred_ms(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=get_start_time,
        )
        graph = RemoteGraph.deploy(graph)

        # Cold start first.
        invocation_id = graph.run(block_until_done=True, x=1)
        output = graph.output(invocation_id, "get_start_time")
        self.assertEqual(len(output), 1)

        # Wait for Server to learn that the created Function Executor is IDLE.
        time.sleep(10)

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
