from typing import Optional

from tensorlake.functions_sdk.data_objects import TensorlakeData
from tensorlake.functions_sdk.object_serializer import get_serializer

from ...proto.function_executor_pb2 import RunTaskRequest, SerializedObject


class FunctionInputs:
    def __init__(
        self, input: TensorlakeData, init_value: Optional[TensorlakeData] = None
    ):
        self.input = input
        self.init_value = init_value


class FunctionInputsLoader:
    def __init__(self, request: RunTaskRequest):
        self._request = request

    def load(self) -> FunctionInputs:
        return FunctionInputs(
            input=self._function_input(),
            init_value=self._accumulator_input(),
        )

    def _function_input(self) -> TensorlakeData:
        return _to_indexify_data(
            self._request.graph_invocation_id, self._request.function_input
        )

    def _accumulator_input(self) -> Optional[TensorlakeData]:
        return (
            _to_indexify_data(
                self._request.graph_invocation_id, self._request.function_init_value
            )
            if self._request.HasField("function_init_value")
            else None
        )


def _to_indexify_data(
    input_id: str, serialized_object: SerializedObject
) -> TensorlakeData:
    return TensorlakeData(
        input_id=input_id,
        payload=(
            serialized_object.bytes
            if serialized_object.HasField("bytes")
            else serialized_object.string
        ),
        encoder=get_serializer(serialized_object.content_type).encoding_type,
    )
