import asyncio
from dataclasses import dataclass

from indexify.proto.executor_api_pb2 import TaskAllocation

from .task_allocation_input import TaskAllocationInput
from .task_allocation_output import TaskAllocationOutput


@dataclass
class TaskAllocationInfo:
    """Object used to track a task allocation during its full lifecycle in the FunctionExecutorController."""

    allocation: TaskAllocation
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
    input: TaskAllocationInput | None = None
    # Output of the task allocation, always set when the allocation is completed.
    output: TaskAllocationOutput | None = None
    # True if the task allocation is fully completed and was added to state reporter.
    is_completed: bool = False
