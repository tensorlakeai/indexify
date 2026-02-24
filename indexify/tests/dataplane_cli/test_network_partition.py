from __future__ import annotations

import concurrent.futures
import os
import sys
import time
import unittest

from tensorlake.applications import (
    Request,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications

# dataplane_cli.testing is only available when running the test directly,
# not when function-executor loads this file from a temp zip.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
try:
    from dataplane_cli.testing import (
        DataplaneProcessContextManager,
        PartitionProxy,
        find_free_port,
        wait_dataplane_startup,
    )
except ModuleNotFoundError:
    pass


@application()
@function()
def fast_func(x: int) -> int:
    return x * 2


@application()
@function()
def slow_func(sleep_secs: float) -> str:
    import time

    time.sleep(sleep_secs)
    return f"done_after_{sleep_secs}s"


SERVER_GRPC_PORT = 8901  # Default server gRPC port

# Timeout for req.output() calls. Tests should never take longer than
# this after the partition heals. Prevents hanging on failure.
OUTPUT_TIMEOUT_SECS = 120


def output_with_timeout(req: Request, timeout: int = OUTPUT_TIMEOUT_SECS):
    """Call req.output() with a timeout. Raises TimeoutError on expiry."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(req.output)
        return future.result(timeout=timeout)


class TestNetworkPartition(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy_applications(__file__)

    def _make_proxy(self) -> PartitionProxy:
        """Create and start a PartitionProxy targeting the server gRPC port."""
        proxy = PartitionProxy("localhost", SERVER_GRPC_PORT)
        proxy.start()
        self.addCleanup(proxy.stop)
        return proxy

    def test_allocation_delivered_after_long_partition(self):
        """Submit req1 (completes) -> partition -> wait 35s -> submit req2
        -> heal -> req2 completes."""
        proxy = self._make_proxy()
        port = find_free_port()

        with DataplaneProcessContextManager(
            config_overrides={
                "server_addr": f"http://localhost:{proxy.port}",
                "http_proxy": {"port": port},
                "monitoring": {"port": find_free_port()},
            },
            keep_std_outputs=True,
        ):
            wait_dataplane_startup(port)

            # req1: verify baseline works
            req1: Request = run_remote_application(fast_func, 1)
            result1 = output_with_timeout(req1)
            self.assertEqual(result1, 2)
            print(f"req1 completed: {result1}")

            # Partition the network
            print("Partitioning network...")
            proxy.partition()
            print("Waiting 35s during partition...")
            time.sleep(35)

            # Submit req2 while partitioned (goes to server via HTTP API,
            # but the dataplane cannot receive it until healed)
            print("Submitting req2 during partition...")
            req2: Request = run_remote_application(fast_func, 21)

            # Heal the network
            print("Healing network...")
            proxy.heal()

            # Wait for the dataplane to reconnect and process req2
            result2 = output_with_timeout(req2)
            self.assertEqual(result2, 42)
            print(f"req2 completed after heal: {result2}")

    def test_inflight_work_completes_after_long_partition(self):
        """Submit slow_func(5s) -> wait 3s -> partition -> function completes
        during partition -> wait 35s -> heal -> result reported -> completes."""
        proxy = self._make_proxy()
        port = find_free_port()

        with DataplaneProcessContextManager(
            config_overrides={
                "server_addr": f"http://localhost:{proxy.port}",
                "http_proxy": {"port": port},
                "monitoring": {"port": find_free_port()},
            },
            keep_std_outputs=True,
        ):
            wait_dataplane_startup(port)

            # Submit slow work
            print("Submitting slow_func(5.0)...")
            req: Request = run_remote_application(slow_func, 5.0)

            # Wait for the function to start executing
            time.sleep(3)

            # Partition while function is still running (~2s remaining)
            print("Partitioning network (function still in-flight)...")
            proxy.partition()

            # Function completes during partition, result is buffered locally
            print("Waiting 35s during partition (function will complete)...")
            time.sleep(35)

            # Heal so the result can be reported back to the server
            print("Healing network...")
            proxy.heal()

            # The buffered result should be reported after reconnection
            result = output_with_timeout(req)
            self.assertEqual(result, "done_after_5.0s")
            print(f"In-flight work completed after heal: {result}")

    def test_transient_partition_no_executor_lapse(self):
        """Submit req1 (completes) -> partition -> wait 10s (< 30s EXECUTOR_TIMEOUT)
        -> heal -> submit req2 -> req2 completes."""
        proxy = self._make_proxy()
        port = find_free_port()

        with DataplaneProcessContextManager(
            config_overrides={
                "server_addr": f"http://localhost:{proxy.port}",
                "http_proxy": {"port": port},
                "monitoring": {"port": find_free_port()},
            },
            keep_std_outputs=True,
        ):
            wait_dataplane_startup(port)

            # Baseline request
            req1: Request = run_remote_application(fast_func, 5)
            result1 = output_with_timeout(req1)
            self.assertEqual(result1, 10)
            print(f"req1 completed: {result1}")

            # Short partition (well under executor timeout)
            print("Partitioning network for 10s...")
            proxy.partition()
            time.sleep(10)

            # Heal before executor timeout lapses
            print("Healing network...")
            proxy.heal()

            # Submit new work -- should succeed without re-registration
            req2: Request = run_remote_application(fast_func, 7)
            result2 = output_with_timeout(req2)
            self.assertEqual(result2, 14)
            print(f"req2 completed after transient partition: {result2}")

    def test_multiple_requests_across_partition(self):
        """Submit req1 (completes) -> partition -> wait 35s -> submit req2,3,4,5
        -> heal -> all 4 complete."""
        proxy = self._make_proxy()
        port = find_free_port()

        with DataplaneProcessContextManager(
            config_overrides={
                "server_addr": f"http://localhost:{proxy.port}",
                "http_proxy": {"port": port},
                "monitoring": {"port": find_free_port()},
            },
            keep_std_outputs=True,
        ):
            wait_dataplane_startup(port)

            # Baseline request
            req1: Request = run_remote_application(fast_func, 1)
            result1 = output_with_timeout(req1)
            self.assertEqual(result1, 2)
            print(f"req1 completed: {result1}")

            # Long partition
            print("Partitioning network for 35s...")
            proxy.partition()
            time.sleep(35)

            # Submit multiple requests during partition
            print("Submitting 4 requests during partition...")
            pending_requests = []
            for i in range(2, 6):
                req: Request = run_remote_application(fast_func, i)
                pending_requests.append((i, req))

            # Heal
            print("Healing network...")
            proxy.heal()

            # All should complete
            for x, req in pending_requests:
                result = output_with_timeout(req)
                self.assertEqual(result, x * 2)
                print(f"req(x={x}) completed: {result}")

            print("All 4 requests completed after partition heal.")

    def test_work_rescheduled_to_healthy_executor(self):
        """Two dataplanes: A (behind proxy), B (direct). Submit slow_func(10s)
        -> wait 3s -> partition A -> wait 35s -> all work completes on B."""
        proxy = self._make_proxy()
        port_a = find_free_port()
        port_b = find_free_port()

        with DataplaneProcessContextManager(
            config_overrides={
                "server_addr": f"http://localhost:{proxy.port}",
                "http_proxy": {"port": port_a},
                "monitoring": {"port": find_free_port()},
            },
            keep_std_outputs=True,
        ):
            wait_dataplane_startup(port_a)

            with DataplaneProcessContextManager(
                config_overrides={
                    "http_proxy": {"port": port_b},
                    "monitoring": {"port": find_free_port()},
                },
                keep_std_outputs=True,
            ):
                wait_dataplane_startup(port_b)

                # Submit slow work
                print("Submitting slow_func(10.0)...")
                req: Request = run_remote_application(slow_func, 10.0)

                # Let work get assigned
                time.sleep(3)

                # Partition dataplane A
                print("Partitioning Dataplane A...")
                proxy.partition()

                # Wait long enough for executor timeout to lapse on A
                print("Waiting 35s for executor lapse on A...")
                time.sleep(35)

                # Work should complete on B (rescheduled by server)
                result = output_with_timeout(req)
                self.assertEqual(result, "done_after_10.0s")
                print(f"Work completed (rescheduled to healthy executor): {result}")

    def test_flapping_network(self):
        """Submit 5 requests -> partition 5s -> heal 5s -> partition 5s
        -> heal -> all complete."""
        proxy = self._make_proxy()
        port = find_free_port()

        with DataplaneProcessContextManager(
            config_overrides={
                "server_addr": f"http://localhost:{proxy.port}",
                "http_proxy": {"port": port},
                "monitoring": {"port": find_free_port()},
            },
            keep_std_outputs=True,
        ):
            wait_dataplane_startup(port)

            # Submit 5 requests
            print("Submitting 5 requests...")
            pending = []
            for i in range(1, 6):
                req: Request = run_remote_application(fast_func, i)
                pending.append((i, req))

            # Flapping: partition -> heal -> partition -> heal
            print("Partition (5s)...")
            proxy.partition()
            time.sleep(5)

            print("Heal (5s)...")
            proxy.heal()
            time.sleep(5)

            print("Partition (5s)...")
            proxy.partition()
            time.sleep(5)

            print("Final heal...")
            proxy.heal()

            # All requests should eventually complete
            for x, req in pending:
                result = output_with_timeout(req)
                self.assertEqual(result, x * 2)
                print(f"req(x={x}) completed: {result}")

            print("All 5 requests completed after flapping network.")

    def test_partition_during_inflight_long_buffered(self):
        """Submit slow_func(2s) -> wait 1s -> partition -> function completes
        ~1s into partition -> wait 34s -> heal -> completes."""
        proxy = self._make_proxy()
        port = find_free_port()

        with DataplaneProcessContextManager(
            config_overrides={
                "server_addr": f"http://localhost:{proxy.port}",
                "http_proxy": {"port": port},
                "monitoring": {"port": find_free_port()},
            },
            keep_std_outputs=True,
        ):
            wait_dataplane_startup(port)

            # Submit work that completes in 2 seconds
            print("Submitting slow_func(2.0)...")
            req: Request = run_remote_application(slow_func, 2.0)

            # Wait 1s so function is mid-execution
            time.sleep(1)

            # Partition -- function has ~1s left
            print("Partitioning network (function has ~1s left)...")
            proxy.partition()

            # Function completes ~1s into partition, result buffered for 34s
            print("Waiting 34s during partition (result buffered locally)...")
            time.sleep(34)

            # Heal so buffered result is reported
            print("Healing network...")
            proxy.heal()

            result = output_with_timeout(req)
            self.assertEqual(result, "done_after_2.0s")
            print(f"Long-buffered in-flight result arrived: {result}")

    def test_cold_start_after_lapse(self):
        """Partition immediately -> wait 35s -> heal -> wait for re-registration
        -> submit new req -> completes."""
        proxy = self._make_proxy()
        port = find_free_port()

        with DataplaneProcessContextManager(
            config_overrides={
                "server_addr": f"http://localhost:{proxy.port}",
                "http_proxy": {"port": port},
                "monitoring": {"port": find_free_port()},
            },
            keep_std_outputs=True,
        ):
            wait_dataplane_startup(port)

            # Partition immediately (before any work is done)
            print("Partitioning network immediately...")
            proxy.partition()

            # Wait for executor lapse on server side
            print("Waiting 35s for executor lapse...")
            time.sleep(35)

            # Heal -- dataplane should re-register
            print("Healing network...")
            proxy.heal()

            # Give time for the dataplane to re-register with the server
            print("Waiting 10s for re-registration...")
            time.sleep(10)

            # Submit new work -- should succeed via cold start
            print("Submitting request after lapse...")
            req: Request = run_remote_application(fast_func, 50)
            result = output_with_timeout(req)
            self.assertEqual(result, 100)
            print(f"Cold start after lapse succeeded: {result}")


if __name__ == "__main__":
    unittest.main()
