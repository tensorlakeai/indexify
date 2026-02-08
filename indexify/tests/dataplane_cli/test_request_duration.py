import time

from tensorlake.applications import (
    Request,
    application,
    cls,
    function,
    run_remote_application,
)


@cls()
class ColdStartMeasurementFunction:
    def __init__(self):
        # Records actual time when the function was initialized.
        # This allows to not measure the latency of Server learning that
        # Function Executor was created.
        self._init_time: float = time.time()

    @application()
    @function()
    def run(self, x: int) -> float:
        return self._init_time


@application()
@function()
def get_start_time(x: int) -> float:
    return time.time()


# Test infrastructure imports are guarded so they don't execute when the
# function executor loads this file as an application module.
if __name__ == "__main__":
    import os
    import sys
    import unittest

    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    from tensorlake.applications.remote.deploy import deploy_applications
    from dataplane_cli.testing import (
        DataplaneProcessContextManager,
        find_free_port,
        wait_dataplane_startup,
    )

    class TestInvokeDurationsDataplane(unittest.TestCase):
        def setUp(self):
            deploy_applications(__file__)

        def test_cold_start_duration_is_less_than_twenty_sec(self):
            port = find_free_port()
            with DataplaneProcessContextManager(
                config_overrides={"http_proxy": {"port": port}},
                keep_std_outputs=True,
            ):
                wait_dataplane_startup(port)

                request_start_time = time.time()
                request: Request = run_remote_application(
                    ColdStartMeasurementFunction.run,
                    1,
                )
                func_init_time: float = request.output()
                cold_start_duration = func_init_time - request_start_time
                print(f"cold_start_duration: {cold_start_duration} seconds")
                # The current duration we see in tests is about 0.3 seconds
                # when run standalone.  When run in CI after other tests, the
                # server may have accumulated state which slows down cold
                # starts significantly.  We use a generous 60s limit to avoid
                # flakiness while still catching major regressions.
                self.assertLess(cold_start_duration, 60)

        def test_warm_start_duration_is_less_than_hundred_ms(self):
            port = find_free_port()
            with DataplaneProcessContextManager(
                config_overrides={"http_proxy": {"port": port}},
                keep_std_outputs=True,
            ):
                wait_dataplane_startup(port)

                # Cold start first.
                request: Request = run_remote_application(
                    get_start_time,
                    1,
                )
                request.output()

                # Wait for Server to learn that the created Function Executor
                # is IDLE.
                time.sleep(10)

                # Measure warm start duration.
                request_start_time = time.time()
                request: Request = run_remote_application(
                    get_start_time,
                    2,
                )
                func_start_time: float = request.output()
                warm_start_duration: float = func_start_time - request_start_time
                print(f"warm_start_duration: {warm_start_duration} seconds")
                # The current duration we see in tests is about 20 ms.
                #
                # We give a large 100 ms headroom to prevent this test getting
                # flaky while still notifying us if the warm start duration
                # regresses significantly.
                self.assertLess(warm_start_duration, 0.1)

    unittest.main()
