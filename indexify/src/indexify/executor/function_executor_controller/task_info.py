import asyncio
from dataclasses import dataclass
from typing import Optional

from tensorlake.function_executor.proto.function_executor_pb2 import SerializedObject

from indexify.proto.executor_api_pb2 import TaskAllocation

from .task_output import TaskOutput


@dataclass
class TaskInfo:
    """Object used to track a task during its full lifecycle in the FunctionExecutorController."""

    allocation: TaskAllocation
    # time.monotonic() timestamp
    start_time: float
    # time.monotonic() timestamp when the task was prepared for execution
    prepared_time: float = 0.0
    # True if the task was cancelled.
    is_cancelled: bool = False
    # aio task that is currently executing a lifecycle step of this task.
    aio_task: Optional[asyncio.Task] = None
    # Downloaded input if function was prepared successfully.
    input: Optional[SerializedObject] = None
    # Downloaded init value if function was prepared successfully and is a reducer.
    init_value: Optional[SerializedObject] = None
    # Output of the task.
    output: Optional[TaskOutput] = None
    # True if the task is fully completed and was added to state reporter.
    is_completed: bool = False
