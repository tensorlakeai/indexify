import mmap
import os
import unittest

from pydantic import BaseModel
from tensorlake.applications import (
    Function,
    Request,
    RequestFailed,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications
from testing import running_on_github_gpu_runner, running_with_gvisor_runtime

TEN_MB = 10 * 1024 * 1024

# This memory overhead is checked in FE integration tests.
FUNCTION_EXECUTOR_MAX_MEMORY_OVERHEAD_MB = 75

if running_with_gvisor_runtime():
    # Gvisor memory overhead is 25 MB, caused by the runsc-gofer process running
    # along with the runsc-sandbox process inside the same cgroup.
    GVISOR_MAX_MEMORY_OVERHEAD_MB = 25
else:
    GVISOR_MAX_MEMORY_OVERHEAD_MB = 0

if running_on_github_gpu_runner():
    # nvidia-smi health checks + nvidia container runtime bind mounts and etc.
    NVIDIA_GPU_MAX_MEMORY_OVERHEAD_MB = 100
else:
    NVIDIA_GPU_MAX_MEMORY_OVERHEAD_MB = 0

# Linux only mmap flag that allows to lock the memory in RAM. Such allocations
# are also known as "system" memory.
MAP_LOCKED: int = 0x02000


MEMORY_ALLOCATOR_PRIVATE_ANONYMOUS = "private_anonymous"
MEMORY_ALLOCATOR_SHARED_ANONYMOUS = "shared_anonymous"
MEMORY_ALLOCATOR_LOCKED_ANONYMOUS = "locked_anonymous"
MEMORY_ALLOCATOR_TMPFS = "tmpfs"


def force_memory_allocation(mem: mmap, size_bytes: int) -> None:
    # Write some data to the shared memory to force its actual allocation by the kernel
    for offset in range(0, size_bytes, mmap.PAGESIZE):
        mem.seek(offset)
        mem.write(b"0")


def mmap_allocations(size_bytes: int, flags: int, access: int) -> None:
    allocs: list[mmap.mmap] = []
    remaining_size = size_bytes
    while remaining_size > 0:
        alloc_size = min(remaining_size, TEN_MB)
        mem = mmap.mmap(
            -1,
            alloc_size,
            flags,
            access,
        )
        force_memory_allocation(mem, alloc_size)
        allocs.append(mem)
        remaining_size -= alloc_size


def allocate_anonymous_private_memory(size_bytes: int) -> None:
    mmap_allocations(
        size_bytes,
        mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS,
        mmap.ACCESS_WRITE | mmap.ACCESS_READ,
    )


def allocate_anonymous_shared_memory(size_bytes: int) -> None:
    mmap_allocations(
        size_bytes,
        mmap.MAP_SHARED | mmap.MAP_ANONYMOUS,
        mmap.ACCESS_WRITE | mmap.ACCESS_READ,
    )


def allocate_anonymous_locked_memory(size_bytes: int) -> None:
    mmap_flags = mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS
    if os.name == "posix" and os.uname().sysname == "Linux":
        mmap_flags |= MAP_LOCKED
    mmap_allocations(
        size_bytes,
        mmap_flags,
        mmap.ACCESS_WRITE | mmap.ACCESS_READ,
    )


def allocate_tmpfs_memory(size_bytes: int) -> None:
    try:
        with open("/dev/shm/allocate_tmpfs_test", "wb") as mem:
            force_memory_allocation(mem, size_bytes)
    finally:
        os.remove("/dev/shm/allocate_tmpfs_test")


def print_debug_memory_info() -> None:
    """Print information about the current memory usage for debugging."""
    with open("/proc/self/status", "r") as f:
        for line in f:
            print(f"DEBUG /proc/self/status: {line.strip()}")
    with open("/proc/meminfo", "r") as f:
        for line in f:
            print(f"DEBUG /proc/meminfo: {line.strip()}")
    with open("/proc/self/limits", "r") as f:
        for line in f:
            print(f"DEBUG /proc/self/limits: {line.strip()}")


def allocate_memory(size_bytes: int, allocator: str, expect_success: bool) -> None:
    try:
        if allocator == MEMORY_ALLOCATOR_PRIVATE_ANONYMOUS:
            allocate_anonymous_private_memory(size_bytes)
        elif allocator == MEMORY_ALLOCATOR_SHARED_ANONYMOUS:
            allocate_anonymous_shared_memory(size_bytes)
        elif allocator == MEMORY_ALLOCATOR_LOCKED_ANONYMOUS:
            allocate_anonymous_locked_memory(size_bytes)
        elif allocator == MEMORY_ALLOCATOR_TMPFS:
            allocate_tmpfs_memory(size_bytes)
        else:
            raise ValueError(f"Unknown allocator: {allocator}")
    except Exception as e:
        if expect_success:
            print(
                f"DEBUG: Error allocating memory (allocator: {allocator}, "
                f"size_bytes: {size_bytes}): {e.__class__.__name__}({e})"
            )
            print_debug_memory_info()
        raise


class AllocateMemoryRequestPayload(BaseModel):
    size_bytes: int
    allocator: str
    expect_success: bool


@application()
@function(memory=2)
def allocate_memory_2_gb(payload: AllocateMemoryRequestPayload) -> str:
    allocate_memory(payload.size_bytes, payload.allocator, payload.expect_success)
    return "success"


@application()
@function(memory=1)
def allocate_memory_1_gb(payload: AllocateMemoryRequestPayload) -> str:
    allocate_memory(payload.size_bytes, payload.allocator, payload.expect_success)
    return "success"


class TestAllocateMemoryBelowAndAboveLimit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy_applications(__file__)

    def test_allocate_memory_below_and_above_memory_limits(self):
        test_cases = [
            (
                MEMORY_ALLOCATOR_PRIVATE_ANONYMOUS,
                1024
                - FUNCTION_EXECUTOR_MAX_MEMORY_OVERHEAD_MB
                - GVISOR_MAX_MEMORY_OVERHEAD_MB
                - NVIDIA_GPU_MAX_MEMORY_OVERHEAD_MB,
                allocate_memory_1_gb,
                True,
            ),
            (MEMORY_ALLOCATOR_PRIVATE_ANONYMOUS, 1024, allocate_memory_1_gb, False),
            (
                MEMORY_ALLOCATOR_SHARED_ANONYMOUS,
                1024
                - FUNCTION_EXECUTOR_MAX_MEMORY_OVERHEAD_MB
                - GVISOR_MAX_MEMORY_OVERHEAD_MB
                - NVIDIA_GPU_MAX_MEMORY_OVERHEAD_MB,
                allocate_memory_1_gb,
                True,
            ),
            (MEMORY_ALLOCATOR_SHARED_ANONYMOUS, 1024, allocate_memory_1_gb, False),
            (
                MEMORY_ALLOCATOR_LOCKED_ANONYMOUS,
                1024
                - FUNCTION_EXECUTOR_MAX_MEMORY_OVERHEAD_MB
                - GVISOR_MAX_MEMORY_OVERHEAD_MB
                - NVIDIA_GPU_MAX_MEMORY_OVERHEAD_MB,
                allocate_memory_1_gb,
                True,
            ),
            (MEMORY_ALLOCATOR_LOCKED_ANONYMOUS, 1024, allocate_memory_1_gb, False),
            (
                MEMORY_ALLOCATOR_TMPFS,
                1024
                - FUNCTION_EXECUTOR_MAX_MEMORY_OVERHEAD_MB
                - GVISOR_MAX_MEMORY_OVERHEAD_MB
                - NVIDIA_GPU_MAX_MEMORY_OVERHEAD_MB,
                allocate_memory_1_gb,
                True,
            ),
            (MEMORY_ALLOCATOR_TMPFS, 1024, allocate_memory_1_gb, False),
            (
                MEMORY_ALLOCATOR_PRIVATE_ANONYMOUS,
                2048
                - FUNCTION_EXECUTOR_MAX_MEMORY_OVERHEAD_MB
                - GVISOR_MAX_MEMORY_OVERHEAD_MB
                - NVIDIA_GPU_MAX_MEMORY_OVERHEAD_MB,
                allocate_memory_2_gb,
                True,
            ),
            (MEMORY_ALLOCATOR_PRIVATE_ANONYMOUS, 2048, allocate_memory_2_gb, False),
            (
                MEMORY_ALLOCATOR_SHARED_ANONYMOUS,
                2048
                - FUNCTION_EXECUTOR_MAX_MEMORY_OVERHEAD_MB
                - GVISOR_MAX_MEMORY_OVERHEAD_MB
                - NVIDIA_GPU_MAX_MEMORY_OVERHEAD_MB,
                allocate_memory_2_gb,
                True,
            ),
            (MEMORY_ALLOCATOR_SHARED_ANONYMOUS, 2048, allocate_memory_2_gb, False),
            (
                MEMORY_ALLOCATOR_LOCKED_ANONYMOUS,
                # mlock ulimit is 1 GB; when running with gVisor, it fails at 1024 MB
                # but succeeds at 1023 MB.
                1023,
                allocate_memory_2_gb,
                True,
            ),
            (
                MEMORY_ALLOCATOR_TMPFS,
                1024,
                allocate_memory_2_gb,
                True,
            ),
            (
                MEMORY_ALLOCATOR_TMPFS,
                1025,
                allocate_memory_2_gb,
                False,
            ),
        ]
        for allocator, size_mb, function, expect_success in test_cases:
            function: Function
            with self.subTest(allocator=allocator, size_mb=size_mb):
                request: Request = run_remote_application(
                    function._function_config.function_name,
                    AllocateMemoryRequestPayload(
                        size_bytes=size_mb * 1024 * 1024,
                        allocator=allocator,
                        expect_success=expect_success,
                    ),
                )
                if expect_success:
                    self.assertEqual(request.output(), "success")
                else:
                    with self.assertRaises(RequestFailed) as cm:
                        request.output()

                    if allocator != MEMORY_ALLOCATOR_TMPFS:
                        # Important UX: the request should fail with OOM.
                        # TMPFS returns IO write error instead of OOM.
                        self.assertEqual("out_of_memory", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
