from typing import Optional

from indexify.function_executor.proto.function_executor_pb2 import (
    SerializedObject,
)

from ..api_objects import Task


class TaskInput:
    """Task with all the resources required to run it."""

    def __init__(
        self,
        task: Task,
        graph: SerializedObject,
        input: SerializedObject,
        init_value: Optional[SerializedObject],
    ):
        self.task: Task = task
        self.graph: SerializedObject = graph
        self.input: SerializedObject = input
        self.init_value: Optional[SerializedObject] = init_value
