from ..proto.function_executor_pb2 import InvocationStateResponse
from ..proto.message_validator import MessageValidator


class ResponseValidator(MessageValidator):
    def __init__(self, response: InvocationStateResponse):
        self._response = response

    def check(self):
        """Validates the request.

        Raises: ValueError: If the response is invalid.
        """
        (MessageValidator(self._response).required_field("request_id"))

        if self._response.HasField("set"):
            (MessageValidator(self._response.set).required_field("success"))
        elif self._response.HasField("get"):
            (
                MessageValidator(self._response.get)
                .required_field("success")
                .required_field("state_key")
                # state_value is optional
            )
        else:
            raise ValueError(f"Unknown response type: {self._response}")
