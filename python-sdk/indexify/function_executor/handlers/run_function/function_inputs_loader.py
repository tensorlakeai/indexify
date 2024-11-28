from typing import Optional

from pydantic import BaseModel

from indexify.function_executor.protocol import BinaryData, RunFunctionRequest
from indexify.functions_sdk.data_objects import IndexifyData
from indexify.functions_sdk.object_serializer import get_serializer


class FunctionInputs(BaseModel):
    input: IndexifyData
    init_value: Optional[IndexifyData] = None


class FunctionInputsLoader:
    def __init__(self, request: RunFunctionRequest):
        self._request = request

    def load(self) -> FunctionInputs:
        # The first function in Graph gets its input from graph invocation payload.
        input = self._graph_invocation_payload()
        if input is None:
            input = self._function_input()
        if input is None:
            raise Exception("No input data found for task")

        return FunctionInputs(
            input=input,
            init_value=self._accomulator_input(),
        )

    def _graph_invocation_payload(self) -> Optional[IndexifyData]:
        return _to_indexify_data(
            self._request.graph_invocation_id, self._request.graph_invocation_payload
        )

    def _function_input(self) -> Optional[IndexifyData]:
        return _to_indexify_data(
            self._request.graph_invocation_id, self._request.function_input
        )

    def _accomulator_input(self) -> Optional[IndexifyData]:
        return _to_indexify_data(
            self._request.graph_invocation_id, self._request.function_init_value
        )


def _to_indexify_data(
    input_id: str, input: Optional[BinaryData]
) -> Optional[IndexifyData]:
    if input is None:
        return None

    return IndexifyData(
        input_id=input_id,
        payload=input.data,
        encoder=get_serializer(input.content_type).encoding_type,
    )
