import logging
from typing import Optional

import grpc

from .handlers.run_function.handler import Handler as RunTaskHandler
from .handlers.run_function.request_validator import (
    RequestValidator as RunTaskRequestValidator,
)
from .proto.function_executor_pb2 import RunTaskRequest, RunTaskResponse
from .proto.function_executor_pb2_grpc import FunctionExecutorServicer


class FunctionExecutorService(FunctionExecutorServicer):
    def __init__(self, indexify_server_address: str, config_path: Optional[str]):
        self._indexify_server_address = indexify_server_address
        self._config_path = config_path

    def RunTask(
        self, request: RunTaskRequest, context: grpc.ServicerContext
    ) -> RunTaskResponse:
        # Customer function code never raises an exception because we catch all of them and add
        # their details to the response. We can only get an exception here if our own code failed.
        # If our code raises an exception the grpc framework converts it into GRPC_STATUS_UNKNOWN
        # error with the exception message. This is good enough for now.
        RunTaskRequestValidator(request=request).check()
        return RunTaskHandler(
            request=request,
            indexify_server_addr=self._indexify_server_address,
            config_path=self._config_path,
        ).run()
