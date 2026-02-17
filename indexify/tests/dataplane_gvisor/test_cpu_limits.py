import multiprocessing
import time
import unittest

from tensorlake.applications import (
    Request,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications


def count_busyloops(duration_sec: int) -> int:
    loops = 0
    deadline = time.monotonic() + duration_sec
    while time.monotonic() < deadline:
        loops += 1
    return loops


def count_busyloops_process_worker(duration_sec: int, result_queue):
    result = count_busyloops(duration_sec)
    result_queue.put(result)


def count_busyloops_using_multiprocessing(process_count: int, duration_sec: int) -> int:
    result_queue = multiprocessing.Queue()
    processes = [
        multiprocessing.Process(
            target=count_busyloops_process_worker, args=(duration_sec, result_queue)
        )
        for _ in range(process_count)
    ]

    for process in processes:
        process.start()

    for process in processes:
        process.join()

    return sum(result_queue.get() for _ in processes)


@application()
@function(cpu=1)
def count_busyloops_one_cpu(duration_sec: int) -> int:
    return count_busyloops_using_multiprocessing(1, duration_sec)


@application()
@function(cpu=2)
def count_busyloops_two_cpus(duration_sec: int) -> int:
    return count_busyloops_using_multiprocessing(2, duration_sec)


class TestCPUBoundPerformaceWithCPULimits(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy_applications(__file__)

    def test_busyloop_performance_scales_with_cpu_limits(self):
        # Use 5 seconds instead of 2 for more stable measurements
        request_one_cpu: Request = run_remote_application(count_busyloops_one_cpu, 5)
        request_two_cpus: Request = run_remote_application(count_busyloops_two_cpus, 5)

        busyloops_one_cpu: int = request_one_cpu.output()
        busyloops_two_cpus: int = request_two_cpus.output()

        perf_ratio_with_two_cpus = busyloops_two_cpus * 1.0 / busyloops_one_cpu
        print(
            f"Performance ratio with two CPUs: {perf_ratio_with_two_cpus:.2f} "
            f"(two CPUs: {busyloops_two_cpus}, one CPU: {busyloops_one_cpu})"
        )
        # Allow wide headroom for gVisor overhead and CI runner noise.
        self.assertLess(perf_ratio_with_two_cpus, 2.5)
        self.assertGreater(perf_ratio_with_two_cpus, 1.1)


if __name__ == "__main__":
    unittest.main()
