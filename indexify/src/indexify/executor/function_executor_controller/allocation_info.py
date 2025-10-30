import asyncio
from dataclasses import dataclass

from indexify.proto.executor_api_pb2 import Allocation

from .allocation_input import AllocationInput
from .allocation_output import AllocationOutput


@dataclass
class AllocationInfo:
    """Object used to track a allocation during its full lifecycle in the FunctionExecutorController."""

    allocation: Allocation
    # Timeout in ms for running customer code.
    allocation_timeout_ms: int
    # time.monotonic() timestamp
    start_time: float
    # time.monotonic() timestamp when the task was prepared for execution
    prepared_time: float = 0.0
    # True if the task was cancelled.
    is_cancelled: bool = False
    # aio task that is currently executing a lifecycle step of this task.
    aio_task: asyncio.Task | None = None
    # Input of the function if task allocation was prepared successfully.
    input: AllocationInput | None = None
    # Output of the task allocation, always set when the allocation is completed.
    output: AllocationOutput | None = None
    # True if the task allocation is fully completed and was added to state reporter.
    is_completed: bool = False
