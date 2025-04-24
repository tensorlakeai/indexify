import asyncio
from typing import Any, List, Optional

import psutil
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

    def __init__(
        self,
        gpu_allocator: NvidiaGPUAllocator,
        function_executors_ephimeral_disks_path: str,
    ):
        """Creates a HostResourcesProvider.

        Args:
            gpu_allocator: The GPU allocator to use for GPU information.
            function_executors_ephimeral_disks_path: The path to file system used as ephimeral disk space by Function Executors.
        """
        self._gpu_allocator: NvidiaGPUAllocator = gpu_allocator
        self._function_executors_ephimeral_disks_path: str = (
            function_executors_ephimeral_disks_path
        )

    async def total_resources(self, logger: Any) -> HostResources:
        """Returns all hardware resources that exist at the host.

        Raises Exception on error.
        """
        # Run psutil library calls in a separate thread to not block the event loop.
        return await asyncio.to_thread(self._total_resources, logger=logger)

    def _total_resources(self, logger: Any) -> HostResources:
        logger = logger.bind(module=__name__)

        # If users disable Hyper-Threading in OS then we'd only see physical cores here.
        # This allows users to control if logical or physical cores are used for resource
        # reporting and for running the functions.
        cpu_count: Optional[int] = psutil.cpu_count(logical=True)
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
