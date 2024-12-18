from typing import Optional

from pydantic import BaseModel

from indexify.function_executor.proto.function_executor_pb2 import (
    RunTaskRequest,
    SerializedObject,
)
from indexify.functions_sdk.data_objects import IndexifyData
from indexify.functions_sdk.object_serializer import get_serializer


class FunctionInputs(BaseModel):
    input: IndexifyData
    init_value: Optional[IndexifyData] = None


class FunctionInputsLoader:
    def __init__(self, request: RunTaskRequest):
        self._request = request

    def load(self) -> FunctionInputs:
        return FunctionInputs(
            input=self._function_input(),
            init_value=self._accumulator_input(),
        )

    def _function_input(self) -> IndexifyData:
        return _to_indexify_data(
            self._request.graph_invocation_id, self._request.function_input
        )

    def _accumulator_input(self) -> Optional[IndexifyData]:
        return (
            _to_indexify_data(
                self._request.graph_invocation_id, self._request.function_init_value
            )
            if self._request.HasField("function_init_value")
            else None
        )


def _to_indexify_data(
    input_id: str, serialized_object: SerializedObject
) -> IndexifyData:
    return IndexifyData(
        input_id=input_id,
        payload=(
            serialized_object.bytes
            if serialized_object.HasField("bytes")
            else serialized_object.string
        ),
        encoder=get_serializer(serialized_object.content_type).encoding_type,
    )
