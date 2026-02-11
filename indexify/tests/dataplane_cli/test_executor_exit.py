import time

from tensorlake.applications import (
    Request,
    application,
    function,
    run_remote_application,
)


@application()
@function()
def success_func(sleep_secs: float) -> str:
    time.sleep(sleep_secs)
    return "success"


# Test infrastructure imports are guarded so they don't execute when the
# function executor loads this file as an application module.
if __name__ == "__main__":
    import os
    import sys
    import unittest
    from typing import List

    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    from dataplane_cli.testing import (
        DataplaneProcessContextManager,
        find_free_port,
        wait_dataplane_startup,
    )
    from tensorlake.applications.remote.deploy import deploy_applications

    class TestExecutorExitDataplane(unittest.TestCase):
        def setUp(self):
            deploy_applications(__file__)

        def test_all_tasks_succeed_when_executor_exits(self):
            """Kill an executor while work is in-flight and verify a second
            executor picks up the rescheduled work so all requests complete."""

            port_a = find_free_port()
            port_b = find_free_port()

            with DataplaneProcessContextManager(
                config_overrides={"http_proxy": {"port": port_b}},
                keep_std_outputs=True,
            ) as executor_b:
                print(f"Started Executor B (survivor) with PID: {executor_b.pid}")
                wait_dataplane_startup(port_b)

                with DataplaneProcessContextManager(
                    config_overrides={"http_proxy": {"port": port_a}},
                    keep_std_outputs=True,
                ) as executor_a:
                    print(
                        f"Started Executor A (will be killed) with PID: {executor_a.pid}"
                    )
                    wait_dataplane_startup(port_a)

                    # Submit requests with a long-enough sleep that they'll
                    # still be in-flight when we kill executor A.
                    requests: List[Request] = []
                    for i in range(10):
                        print(f"Submitting request {i}")
                        request: Request = run_remote_application(
                            success_func,
                            2.0,
                        )
                        requests.append(request)

                    # Give the server a moment to assign work to executor A.
                    time.sleep(3)

                # executor A is now terminated (exited the with-block) while
                # work is still in-flight. Executor B is still running and
                # should pick up the rescheduled work.
                print(
                    "Executor A killed. Waiting for all requests to complete via Executor B..."
                )

                for request in requests:
                    print(f"Waiting for request {request.id}...")
                    output: str = request.output()
                    print(f"Request {request.id}: {output}")
                    self.assertEqual(output, "success")

                print("All requests completed successfully after executor exit.")

    unittest.main()
