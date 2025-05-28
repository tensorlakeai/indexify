from enum import Enum
from typing import Optional

from indexify.executor.function_executor.function_executor import (
    CustomerError,
    FunctionExecutor,
)

from .task_info import TaskInfo


class EventType(Enum):
    FUNCTION_EXECUTOR_CREATED = 1
    FUNCTION_EXECUTOR_DESTROYED = 2
    SHUTDOWN_INITIATED = 3
    TASK_PREPARATION_FINISHED = 4
    SCHEDULE_TASK_EXECUTION = 5
    TASK_EXECUTION_FINISHED = 6
    TASK_OUTPUT_UPLOAD_FINISHED = 7


class BaseEvent:
    """
    Base class for events in the FunctionExecutorController.
    This class can be extended to create specific event types.
    """

    def __init__(self, event_type: EventType):
        self.event_type = event_type

    def __str__(self) -> str:
        return f"Event(type={self.event_type.name})"


class FunctionExecutorCreated(BaseEvent):
    """
    Event indicating that Function Executor got created or failed.

    If the error is CustomerError, it indicates an error in customer code.
    The function_executor field is None if any errors happened.
    """

    def __init__(
        self,
        function_executor: Optional[FunctionExecutor] = None,
        customer_error: Optional[CustomerError] = None,
    ):
        super().__init__(EventType.FUNCTION_EXECUTOR_CREATED)
        self.function_executor: Optional[FunctionExecutor] = function_executor
        self.customer_error: Optional[CustomerError] = customer_error


class FunctionExecutorDestroyed(BaseEvent):
    """
    Event indicating that Function Executor has been destroyed.
    """

    def __init__(self, is_success: bool):
        super().__init__(EventType.FUNCTION_EXECUTOR_DESTROYED)
        self.is_success: bool = is_success

    def __str__(self) -> str:
        return f"Event(type={self.event_type.name}, " f"is_success={self.is_success}"


class ShutdownInitiated(BaseEvent):
    """
    Event indicating that Function Executor shutdown has been initiated.
    """

    def __init__(self):
        super().__init__(EventType.SHUTDOWN_INITIATED)


class TaskPreparationFinished(BaseEvent):
    """
    Event indicating that a task has been prepared for execution or failed to do that.
    """

    def __init__(
        self,
        task_info: TaskInfo,
        is_success: bool,
    ):
        super().__init__(EventType.TASK_PREPARATION_FINISHED)
        self.task_info: TaskInfo = task_info
        self.is_success: bool = is_success

    def __str__(self) -> str:
        return (
            f"Event(type={self.event_type.name}, "
            f"task_id={self.task_info.task.id}, "
            f"allocation_id={self.task_info.allocation_id}), "
            f"is_success={self.is_success}"
        )


class ScheduleTaskExecution(BaseEvent):
    """
    Event indicating that a task execution has been scheduled.
    """

    def __init__(self):
        super().__init__(EventType.SCHEDULE_TASK_EXECUTION)


class TaskExecutionFinished(BaseEvent):
    """
    Event indicating that a task execution has been finished on Function Executor.
    """

    def __init__(
        self,
        task_info: TaskInfo,
        function_executor_is_reusable: bool,
    ):
        super().__init__(EventType.TASK_EXECUTION_FINISHED)
        self.task_info: TaskInfo = task_info
        # False if the task finished in a bad way and the Function Executor is now in undefined state
        # and should not be used to run tasks anymore.
        self.function_executor_is_reusable: bool = function_executor_is_reusable

    def __str__(self) -> str:
        return (
            f"Event(type={self.event_type.name}, "
            f"task_id={self.task_info.task.id}, "
            f"allocation_id={self.task_info.allocation_id})"
        )


class TaskOutputUploadFinished(BaseEvent):
    """
    Event indicating that a task output has been uploaded.
    """

    def __init__(self, task_info: TaskInfo, is_success: bool):
        super().__init__(EventType.TASK_OUTPUT_UPLOAD_FINISHED)
        self.task_info: TaskInfo = task_info
        self.is_success: bool = is_success

    def __str__(self) -> str:
        return (
            f"Event(type={self.event_type.name}, "
            f"task_id={self.task_info.task.id}, "
            f"allocation_id={self.task_info.allocation_id}), "
            f"is_success={self.is_success}"
        )
