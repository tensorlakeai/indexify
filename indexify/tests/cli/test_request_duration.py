import time
import unittest

import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy


@tensorlake.cls()
class ColdStartMeasurementFunction:
    def __init__(self):
        # Records actual time when the function was initialized.
        # This allows to not measure the latency of Server learning that Function Executor was created.
        self._init_time: float = time.time()

    @tensorlake.api()
    @tensorlake.function()
    def run(self, x: int) -> float:
        return self._init_time


@tensorlake.api()
@tensorlake.function()
def get_start_time(x: int) -> float:
    return time.time()


class TestInvokeDurations(unittest.TestCase):
    def setUp(self):
        deploy(__file__)

    def test_cold_start_duration_is_less_than_ten_sec(self):
        request_start_time = time.time()
        request: tensorlake.Request = tensorlake.call_remote_api(
            ColdStartMeasurementFunction.run,
            1,
        )
        func_init_time: float = request.output()
        cold_start_duration = func_init_time - request_start_time
        print(f"cold_start_duration: {cold_start_duration} seconds")
        # The current duration we see in tests is about 3 seconds
        # with p100 of 5 secs.
        #
        # We give a large headroom to prevent this test getting flaky
        # while still notifiying us if the cold start duration regresses
        # significantly.
        self.assertLess(cold_start_duration, 10)

    def test_warm_start_duration_is_less_than_hundred_ms(self):
        # Cold start first.
        request: tensorlake.Request = tensorlake.call_remote_api(
            get_start_time,
            1,
        )
        func_start_time: float = request.output()

        # Wait for Server to learn that the created Function Executor is IDLE.
        time.sleep(10)

        # Measure warm start duration.
        request_start_time = time.time()
        request: tensorlake.Request = tensorlake.call_remote_api(
            get_start_time,
            2,
        )
        func_start_time: float = request.output()
        warm_start_duration: float = func_start_time - request_start_time
        print(f"warm_start_duration: {warm_start_duration} seconds")
        # The current duration we see in tests is about 20 ms.
        #
        # We give a large 100 ms headroom to prevent this test getting flaky
        # while still notifiying us if the warm start duration regresses
        # significantly.
        self.assertLess(warm_start_duration, 0.1)


if __name__ == "__main__":
    unittest.main()
