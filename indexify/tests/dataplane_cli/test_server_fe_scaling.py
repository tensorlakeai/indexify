import os
import time

from tensorlake.applications import (
    Request,
    application,
    function,
    run_remote_application,
)


def function_executor_id() -> int:
    """Returns the PID of the current function executor process."""
    return os.getpid()


@application()
@function()
def test_function_1(sleep_secs: float) -> int:
    time.sleep(sleep_secs)
    return function_executor_id()


@application()
@function()
def test_function_2(sleep_secs: float) -> int:
    time.sleep(sleep_secs)
    return function_executor_id()


# Server side configuration.
_FE_ALLOCATIONS_QUEUE_SIZE = 1


# Test infrastructure imports are guarded so they don't execute when the
# function executor loads this file as an application module.
if __name__ == "__main__":
    import sys
    import unittest
    from typing import List, Set

    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    from dataplane_cli.testing import (
        DataplaneProcessContextManager,
        find_free_port,
        wait_dataplane_startup,
    )
    from tensorlake.applications.remote.deploy import deploy_applications

    class TestServerFunctionExecutorScalingDataplane(unittest.TestCase):
        def test_server_scales_up_function_executors_for_slow_function(self):
            """When running long functions, the server should scale up and
            create new FEs because existing ones are busy."""
            deploy_applications(__file__)

            port = find_free_port()
            with DataplaneProcessContextManager(
                config_overrides={"http_proxy": {"port": port}},
                keep_std_outputs=True,
            ):
                wait_dataplane_startup(port)

                # Submit several slow requests so the server is forced to
                # scale up beyond a single FE.  The exact count depends on
                # available CPU cores and RAM, so we only assert that more
                # than one FE was used.
                _NUM_REQUESTS = 4
                requests: List[Request] = []
                for _ in range(_NUM_REQUESTS):
                    request: Request = run_remote_application(
                        test_function_1,
                        5,
                    )
                    requests.append(request)

                fe_ids: Set[int] = set()
                for request in requests:
                    fe_ids.add(request.output())

                print(f"FE IDs observed: {fe_ids} (count: {len(fe_ids)})")
                self.assertGreaterEqual(len(fe_ids), 2)

        def test_server_uses_the_same_function_executor_if_fe_task_queue_doesnt_overflow(
            self,
        ):
            """When running fast functions, the server should reuse the same
            FE because its task queue never overflows."""
            deploy_applications(__file__)

            port = find_free_port()
            with DataplaneProcessContextManager(
                config_overrides={"http_proxy": {"port": port}},
                keep_std_outputs=True,
            ):
                wait_dataplane_startup(port)

                requests: List[Request] = []
                for _ in range(_FE_ALLOCATIONS_QUEUE_SIZE):
                    request: Request = run_remote_application(
                        test_function_2,
                        0.01,
                    )
                    requests.append(request)

                fe_ids: Set[int] = set()
                for request in requests:
                    fe_ids.add(request.output())

                print(f"FE IDs observed: {fe_ids} (count: {len(fe_ids)})")
                self.assertEqual(len(fe_ids), 1)

    unittest.main()
