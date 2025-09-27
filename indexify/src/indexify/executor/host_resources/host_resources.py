from dataclasses import dataclass
from typing import Any, List

import psutil

from .nvidia_gpu import NvidiaGPUInfo
from .nvidia_gpu_allocator import NvidiaGPUAllocator


@dataclass
class HostResources:
    cpu_count: int
    memory_mb: int
    disk_mb: int
    gpus: List[NvidiaGPUInfo]


class HostResourcesProvider:
    """
    HostResourcesProvider is a class that provides information about the host resources.
    """

    def __init__(
        self,
        gpu_allocator: NvidiaGPUAllocator,
        function_executors_ephimeral_disks_path: str,
        host_overhead_cpus: int,
        host_overhead_memory_gb: int,
        host_overhead_function_executors_ephimeral_disks_gb: int,
    ):
        """Creates a HostResourcesProvider.

        Args:
            gpu_allocator: The GPU allocator to use for GPU information.
            function_executors_ephimeral_disks_path: The path to file system used as ephimeral disk space by Function Executors.
            host_overhead_cpus: The number of CPUs reserved for use by host (can't be used by Function Executors).
            host_overhead_memory_gb: The amount of memory reserved for use by host (can't be used by Function Executors).
            host_overhead_function_executors_ephimeral_disks_gb: The amount of ephimeral disk space reserved for use by host (can't be used by Function Executors).
        """
        self._gpu_allocator: NvidiaGPUAllocator = gpu_allocator
        self._function_executors_ephimeral_disks_path: str = (
            function_executors_ephimeral_disks_path
        )
        self._host_overhead_cpus: int = host_overhead_cpus
        self._host_overhead_memory_gb: int = host_overhead_memory_gb
        self._host_overhead_function_executors_ephimeral_disks_gb: int = (
            host_overhead_function_executors_ephimeral_disks_gb
        )

    def total_host_resources(self, logger: Any) -> HostResources:
        """Returns all hardware resources that exist at the host.

        Raises Exception on error.
        """
        logger = logger.bind(module=__name__)

        # If users disable Hyper-Threading in OS then we'd only see physical cores here.
        # This allows users to control if logical or physical cores are used for resource
        # reporting and for running the functions.
        cpu_count: int | None = psutil.cpu_count(logical=True)
        if cpu_count is None:
            logger.warning(
                "Unable to determine CPU count. Defaulting to 0.",
                cpu_count=cpu_count,
            )
            cpu_count = 0

        memory_mb: int = int(psutil.virtual_memory().total / 1024 / 1024)
        disk_mb = int(
            psutil.disk_usage(self._function_executors_ephimeral_disks_path).total
            / 1024
            / 1024
        )
        all_gpus: List[NvidiaGPUInfo] = self._gpu_allocator.list_all()

        return HostResources(
            cpu_count=cpu_count,
            memory_mb=memory_mb,
            disk_mb=disk_mb,
            gpus=all_gpus,
        )

    def total_function_executor_resources(self, logger: Any) -> HostResources:
        """Returns all hardware resources on the host that are usable by Function Executors.

        Raises Exception on error.
        """
        total_resources: HostResources = self.total_host_resources(logger=logger)
        return HostResources(
            cpu_count=max(0, total_resources.cpu_count - self._host_overhead_cpus),
            memory_mb=max(
                0, total_resources.memory_mb - self._host_overhead_memory_gb * 1024
            ),
            disk_mb=max(
                0,
                total_resources.disk_mb
                - self._host_overhead_function_executors_ephimeral_disks_gb * 1024,
            ),
            gpus=total_resources.gpus,
        )
