from typing import Any, List

from .nvidia_gpu import NvidiaGPUInfo, fetch_nvidia_gpu_infos, nvidia_gpus_are_available


class NvidiaGPUAllocator:
    """NvidiaGPUAllocator is a class that manages the allocation and deallocation of GPUs."""

    def __init__(self, logger: Any):
        gpu_infos: List[NvidiaGPUInfo] = []

        if nvidia_gpus_are_available():
            gpu_infos = fetch_nvidia_gpu_infos(logger)
            logger.bind(module=__name__).info(
                "Fetched information about NVIDIA GPUs:", info=gpu_infos
            )

        self._all_gpus: List[NvidiaGPUInfo] = gpu_infos
        self._free_gpus: List[NvidiaGPUInfo] = list(gpu_infos)

    def allocate(self, count: int, logger: Any) -> List[NvidiaGPUInfo]:
        """
        Allocates a specified number of GPUs.

        Args:
            count (int): The number of GPUs to allocate.

        Returns:
            List[NvidiaGPUInfo]: A list of allocated GPUs. The list is empty if count is 0.

        Raises:
            ValueError: If the requested number of GPUs exceeds free GPUs.
            Exception: If an error occurs during allocation.
        """
        if count > len(self._free_gpus):
            raise ValueError(
                f"Not enough free GPUs available, requested={count}, available={len(self._free_gpus)}"
            )

        allocated_gpus: List[NvidiaGPUInfo] = []
        for _ in range(count):
            allocated_gpus.append(self._free_gpus.pop())

        if len(allocated_gpus) > 0:
            logger.bind(module=__name__).info("allocated GPUs:", gpus=allocated_gpus)

        return allocated_gpus

    def deallocate(self, gpus: List[NvidiaGPUInfo], logger: Any) -> None:
        self._free_gpus.extend(gpus)

        if len(gpus) > 0:
            logger.bind(module=__name__).info("deallocated GPUs:", gpus=gpus)

    def list_all(self) -> List[NvidiaGPUInfo]:
        return list(self._all_gpus)  # Return a copy to avoid external modification

    def list_free(self) -> List[NvidiaGPUInfo]:
        return list(self._free_gpus)  # Return a copy to avoid external modification
