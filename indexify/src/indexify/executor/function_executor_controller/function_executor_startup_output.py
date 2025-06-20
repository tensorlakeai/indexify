from typing import Optional

from indexify.proto.executor_api_pb2 import (
    DataPayload,
    FunctionExecutorDescription,
    FunctionExecutorTerminationReason,
)


class FunctionExecutorStartupOutput:
    def __init__(
        self,
        function_executor_description: FunctionExecutorDescription,
        termination_reason: FunctionExecutorTerminationReason = None,  # None if FE created successfully (wasn't terminated)
        stdout: Optional[DataPayload] = None,
        stderr: Optional[DataPayload] = None,
    ):
        self.function_executor_description = function_executor_description
        self.termination_reason = termination_reason
        self.stdout = stdout
        self.stderr = stderr
