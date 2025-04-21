from typing import List

from pydantic import BaseModel

from .nvidia_gpu import NvidiaGPUInfo
from .nvidia_gpu_allocator import NvidiaGPUAllocator
from psutil import cpu_count, virtual_memory, disk_usage

class HostResources(BaseModel):
    cpu_count: int
    memory_mb: int
    disk_mb: int
    gpus: List[NvidiaGPUInfo]


class HostResourcesProvider:
    """
    HostResourcesProvider is a class that provides information about the host resources.
    """

    def __init__(self, gpu_allocator: NvidiaGPUAllocator):
        self._gpu_allocator: NvidiaGPUAllocator = gpu_allocator

    def total_resources(self, logger) -> HostResources:
        """Returns all hardware resources that exist at the host.

        Raises Exception on error.
        """
        logger = logger.bind(module=__name__)

        return self.free_resources(logger)

    def free_resources(self, logger) -> HostResources:
        """Returns all hardware resources that are free at the host.

        Raises Exception on error.
        """
        logger = logger.bind(module=__name__)
        
        return HostResources(
            cpu_count=int(cpu_count()),
            memory_mb=int(virtual_memory().total / 1024 / 1024),
            disk_mb=int(disk_usage("/").total / 1024 / 1024),
            gpus=self._gpu_allocator.list_all(),
        )
