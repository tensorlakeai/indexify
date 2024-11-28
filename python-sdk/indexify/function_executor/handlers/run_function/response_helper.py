from typing import List

from indexify.function_executor.protocol import (
    BinaryData,
    FunctionOutput,
    RouterOutput,
    RunFunctionResponse,
)
from indexify.functions_sdk.data_objects import IndexifyData
from indexify.functions_sdk.indexify_functions import (
    FunctionCallResult,
    RouterCallResult,
)
from indexify.functions_sdk.object_serializer import get_serializer


class ResponseHelper:
    """Helper class for generating RunFunctionResponse."""

    def __init__(self, task_id: str):
        self._task_id = task_id

    def function_response(
        self,
        result: FunctionCallResult,
        is_reducer: bool,
        stdout: str = "",
        stderr: str = "",
    ) -> RunFunctionResponse:
        if result.traceback_msg is None:
            return RunFunctionResponse(
                task_id=self._task_id,
                function_output=self._to_function_output(result.ser_outputs),
                router_output=None,
                stdout=stdout,
                stderr=stderr,
                is_reducer=is_reducer,
                success=True,
            )
        else:
            return self.failure_response(
                message=result.traceback_msg,
                stdout=stdout,
                stderr=stderr,
            )

    def router_response(
        self,
        result: RouterCallResult,
        stdout: str = "",
        stderr: str = "",
    ) -> RunFunctionResponse:
        if result.traceback_msg is None:
            return RunFunctionResponse(
                task_id=self._task_id,
                function_output=None,
                router_output=RouterOutput(edges=result.edges),
                stdout=stdout,
                stderr=stderr,
                is_reducer=False,
                success=True,
            )
        else:
            return self.failure_response(
                message=result.traceback_msg,
                stdout=stdout,
                stderr=stderr,
            )

    def failure_response(
        self, message: str, stdout: str, stderr: str
    ) -> RunFunctionResponse:
        stderr = "\n".join([stderr, message])
        return RunFunctionResponse(
            task_id=self._task_id,
            function_output=None,
            router_output=None,
            stdout=stdout,
            stderr=stderr,
            is_reducer=False,
            success=False,
        )

    def _to_function_output(self, outputs: List[IndexifyData]) -> FunctionOutput:
        output = FunctionOutput(outputs=[])
        for ix_data in outputs:
            output.outputs.append(
                BinaryData(
                    data=ix_data.payload,
                    content_type=get_serializer(ix_data.encoder).content_type,
                )
            )
        return output
