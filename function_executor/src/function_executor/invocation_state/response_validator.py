from ..proto.function_executor_pb2 import InvocationStateResponse
from ..proto.message_validator import MessageValidator


class ResponseValidator(MessageValidator):
    def __init__(self, response: InvocationStateResponse):
        self._response = response

    def check(self):
        """Validates the request.

        Raises: ValueError: If the response is invalid.
        """
        (
            MessageValidator(self._response)
            .required_field("request_id")
            .required_field("success")
        )

        if self._response.HasField("set"):
            pass
        elif self._response.HasField("get"):
            (
                MessageValidator(self._response.get)
                .required_field("key")
                .optional_serialized_object("value")
            )
        else:
            raise ValueError(f"Unknown response type: {self._response}")
