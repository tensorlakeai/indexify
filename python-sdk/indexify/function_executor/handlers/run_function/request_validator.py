from typing import Any

from indexify.function_executor.proto.function_executor_pb2 import (
    RunTaskRequest,
)
from indexify.function_executor.proto.message_validator import MessageValidator


class RequestValidator:
    def __init__(self, request: RunTaskRequest):
        self._request = request
        self._message_validator = MessageValidator(request)

    def check(self):
        """Validates the request.

        Raises: ValueError: If the request is invalid.
        """
        (
            self._message_validator.required_field("graph_invocation_id")
            .required_field("task_id")
            .optional_serialized_object("function_init_value")
        )

        if self._request.HasField("graph_invocation_payload"):
            # The first function invocation in the graph gets its input from the graph invocation payload.
            self._message_validator.required_serialized_object(
                "graph_invocation_payload"
            )
        else:
            # All other function invocations get their input from the previous function invocation.
            self._message_validator.required_serialized_object("function_input")
