from typing import List

from pydantic import BaseModel

from .nvidia_gpu import NvidiaGPUInfo
from .nvidia_gpu_allocator import NvidiaGPUAllocator


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

        return HostResources(
            cpu_count=0,  # TODO: Implement for Linux and MacOS hosts
            memory_mb=0,  # TODO: Implement for Linux and MacOS hosts
            disk_mb=0,  # TODO: Implement for Linux and MacOS hosts
            gpus=self._gpu_allocator.list_all(),
        )

    def free_resources(self, logger) -> HostResources:
        """Returns all hardware resources that are free at the host.

        Raises Exception on error.
        """
        logger = logger.bind(module=__name__)

        return HostResources(
            cpu_count=0,  # TODO: Implement for Linux and MacOS hosts
            memory_mb=0,  # TODO: Implement for Linux and MacOS hosts
            disk_mb=0,  # TODO: Implement for Linux and MacOS hosts
            gpus=self._gpu_allocator.list_free(),
        )
