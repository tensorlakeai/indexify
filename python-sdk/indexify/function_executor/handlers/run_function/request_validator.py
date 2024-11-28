from typing import Any

from indexify.function_executor.proto.function_executor_pb2 import (
    RunTaskRequest,
    SerializedObject,
)


class RequestValidator:
    def __init__(self, request: RunTaskRequest):
        self._request = request

    def check(self):
        """Validates the request.

        Raises: ValueError: If the request is invalid.
        """
        _required_field(self._request, "namespace")
        _required_field(self._request, "graph_name")
        _required_field(self._request, "graph_version")
        _required_field(self._request, "graph_invocation_id")
        _required_field(self._request, "function_name")
        _required_field(self._request, "task_id")
        _required_field(self._request, "graph")
        _check_serialized_object(self._request.graph)
        if self._request.HasField("graph_invocation_payload"):
            # The first function invocation in the graph gets its input from the graph invocation payload.
            _check_serialized_object(self._request.graph_invocation_payload)
        else:
            # All other function invocations get their input from the previous function invocation.
            _required_field(self._request, "function_input")
            _check_serialized_object(self._request.function_input)
        if self._request.HasField("function_init_value"):
            _check_serialized_object(self._request.function_init_value)


def _required_field(request: RunTaskRequest, field_name: str):
    if not request.HasField(field_name):
        raise ValueError(f"Field '{field_name}' is required in RunTaskRequest")


def _check_serialized_object(serialized_object: SerializedObject):
    if not serialized_object.HasField("string") and not serialized_object.HasField(
        "bytes"
    ):
        raise ValueError("oneof 'data' is requred in SerializedObject")
    if not serialized_object.HasField("content_type"):
        raise ValueError("Field 'content_type' is requred in SerializedObject")
