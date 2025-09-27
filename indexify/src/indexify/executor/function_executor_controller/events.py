from enum import Enum
from typing import List

from indexify.executor.function_executor.function_executor import (
    FunctionExecutor,
)
from indexify.proto.executor_api_pb2 import FunctionExecutorTerminationReason

from .task_allocation_info import TaskAllocationInfo


class EventType(Enum):
    FUNCTION_EXECUTOR_CREATED = 1
    FUNCTION_EXECUTOR_TERMINATED = 2
    SHUTDOWN_INITIATED = 3
    TASK_ALLOCATION_PREPARATION_FINISHED = 4
    SCHEDULE_TASK_ALLOCATION_EXECUTION = 5
    TASK_ALLOCATION_EXECUTION_FINISHED = 6
    TASK_ALLOCATION_FINALIZATION_FINISHED = 7


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

    The function_executor field is None if the function executor creation failed.
    In this case the fe_termination_reason field is set to the reason why.
    """

    def __init__(
        self,
        function_executor: FunctionExecutor | None,
        fe_termination_reason: FunctionExecutorTerminationReason | None,
    ):
        super().__init__(EventType.FUNCTION_EXECUTOR_CREATED)
        self.function_executor: FunctionExecutor | None = function_executor
        self.fe_termination_reason: FunctionExecutorTerminationReason | None = (
            fe_termination_reason
        )


class FunctionExecutorTerminated(BaseEvent):
    """
    Event indicating that Function Executor has been terminated (destroyed).
    """

    def __init__(
        self,
        is_success: bool,
        fe_termination_reason: FunctionExecutorTerminationReason,
        allocation_ids_caused_termination: List[str],
    ):
        super().__init__(EventType.FUNCTION_EXECUTOR_TERMINATED)
        self.is_success: bool = is_success
        self.fe_termination_reason: FunctionExecutorTerminationReason = (
            fe_termination_reason
        )
        self.allocation_ids_caused_termination: List[str] = (
            allocation_ids_caused_termination
        )

    def __str__(self) -> str:
        return (
            f"Event(type={self.event_type.name}, "
            f"is_success={self.is_success}, "
            f"fe_termination_reason={FunctionExecutorTerminationReason.Name(self.fe_termination_reason)}, "
            f"allocation_ids_caused_termination={self.allocation_ids_caused_termination})"
        )


class ShutdownInitiated(BaseEvent):
    """
    Event indicating that Function Executor shutdown has been initiated.
    """

    def __init__(self):
        super().__init__(EventType.SHUTDOWN_INITIATED)


class TaskAllocationPreparationFinished(BaseEvent):
    """
    Event indicating that a task allocation has been prepared for execution or failed to do that.
    """

    def __init__(
        self,
        alloc_info: TaskAllocationInfo,
        is_success: bool,
    ):
        super().__init__(EventType.TASK_ALLOCATION_PREPARATION_FINISHED)
        self.alloc_info: TaskAllocationInfo = alloc_info
        self.is_success: bool = is_success

    def __str__(self) -> str:
        return (
            f"Event(type={self.event_type.name}, "
            f"task_id={self.alloc_info.allocation.task.id}, "
            f"allocation_id={self.alloc_info.allocation.allocation_id}), "
            f"is_success={self.is_success}"
        )


class ScheduleTaskAllocationExecution(BaseEvent):
    """
    Event indicating that a task allocation has been scheduled.
    """

    def __init__(self):
        super().__init__(EventType.SCHEDULE_TASK_ALLOCATION_EXECUTION)


class TaskAllocationExecutionFinished(BaseEvent):
    """
    Event indicating that a task allocation execution has been finished on Function Executor.
    """

    def __init__(
        self,
        alloc_info: TaskAllocationInfo,
        function_executor_termination_reason: (
            FunctionExecutorTerminationReason | None
        ),  # type: Optional[FunctionExecutorTerminationReason]
    ):
        super().__init__(EventType.TASK_ALLOCATION_EXECUTION_FINISHED)
        self.alloc_info: TaskAllocationInfo = alloc_info
        # Not None if the FE needs to get destroyed after running the task.
        self.function_executor_termination_reason = function_executor_termination_reason

    def __str__(self) -> str:
        function_executor_termination_reason_str: str = (
            "None"
            if self.function_executor_termination_reason is None
            else FunctionExecutorTerminationReason.Name(
                self.function_executor_termination_reason
            )
        )
        return (
            f"Event(type={self.event_type.name}, "
            f"task_id={self.alloc_info.allocation.task.id}, "
            f"allocation_id={self.alloc_info.allocation.allocation_id}), "
            f"function_executor_termination_reason={function_executor_termination_reason_str}"
        )


class TaskAllocationFinalizationFinished(BaseEvent):
    """
    Event indicating that a task allocation finalization is finished.
    """

    def __init__(self, alloc_info: TaskAllocationInfo, is_success: bool):
        super().__init__(EventType.TASK_ALLOCATION_FINALIZATION_FINISHED)
        self.alloc_info: TaskAllocationInfo = alloc_info
        self.is_success: bool = is_success

    def __str__(self) -> str:
        return (
            f"Event(type={self.event_type.name}, "
            f"task_id={self.alloc_info.allocation.task.id}, "
            f"allocation_id={self.alloc_info.allocation.allocation_id}), "
            f"is_success={self.is_success}"
        )
