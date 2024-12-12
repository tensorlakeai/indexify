from indexify.function_executor.proto.message_validator import MessageValidator

from .proto.function_executor_pb2 import InitializeRequest


class InitializeRequestValidator:
    def __init__(self, request: InitializeRequest):
        self._request = request
        self._message_validator = MessageValidator(request)

    def check(self):
        """Validates the request.

        Raises: ValueError: If the request is invalid.
        """
        (
            self._message_validator.required_field("namespace")
            .required_field("graph_name")
            .required_field("graph_version")
            .required_field("function_name")
            .required_serialized_object("graph")
        )
