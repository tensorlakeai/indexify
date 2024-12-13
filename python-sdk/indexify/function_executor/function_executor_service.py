from typing import Optional, Union

import grpc
import structlog

from indexify.functions_sdk.indexify_functions import (
    IndexifyFunction,
    IndexifyRouter,
)
from indexify.functions_sdk.object_serializer import get_serializer

from .handlers.run_function.handler import Handler as RunTaskHandler
from .handlers.run_function.request_validator import (
    RequestValidator as RunTaskRequestValidator,
)
from .initialize_request_validator import InitializeRequestValidator
from .proto.function_executor_pb2 import (
    InitializeRequest,
    InitializeResponse,
    RunTaskRequest,
    RunTaskResponse,
)
from .proto.function_executor_pb2_grpc import FunctionExecutorServicer


class FunctionExecutorService(FunctionExecutorServicer):
    def __init__(self, indexify_server_address: str, config_path: Optional[str]):
        self._indexify_server_address = indexify_server_address
        self._config_path = config_path
        self._logger = structlog.get_logger(module=__name__)
        self._namespace: Optional[str] = None
        self._graph_name: Optional[str] = None
        self._graph_version: Optional[int] = None
        self._function_name: Optional[str] = None
        self._function: Optional[Union[IndexifyFunction, IndexifyRouter]] = None

    def initialize(
        self, request: InitializeRequest, context: grpc.ServicerContext
    ) -> InitializeResponse:
        request_validator: InitializeRequestValidator = InitializeRequestValidator(
            request
        )
        request_validator.check()

        self._namespace = request.namespace
        self._graph_name = request.graph_name
        self._graph_version = request.graph_version
        self._function_name = request.function_name
        # The function is only loaded once per Function Executor. It's important to use a single
        # loaded function so all the tasks when executed are sharing the same memory. This allows
        # implementing smart caching in customer code. E.g. load a model into GPU only once and
        # share the model's file descriptor between all tasks or download function configuration
        # only once.
        graph_serializer = get_serializer(request.graph.content_type)
        graph = graph_serializer.deserialize(request.graph.bytes)
        self._function = graph_serializer.deserialize(graph[request.function_name])

        self._logger = self._logger.bind(
            namespace=request.namespace,
            graph_name=request.graph_name,
            graph_version=str(request.graph_version),
            function_name=request.function_name,
        )
        self._logger.info("initialized function executor service")

        return InitializeResponse(success=True)

    def run_task(
        self, request: RunTaskRequest, context: grpc.ServicerContext
    ) -> RunTaskResponse:
        # Customer function code never raises an exception because we catch all of them and add
        # their details to the response. We can only get an exception here if our own code failed.
        # If our code raises an exception the grpc framework converts it into GRPC_STATUS_UNKNOWN
        # error with the exception message. Differentiating errors is not needed for now.
        RunTaskRequestValidator(request=request).check()
        return RunTaskHandler(
            request=request,
            namespace=self._namespace,
            graph_name=self._graph_name,
            graph_version=self._graph_version,
            function_name=self._function_name,
            function=self._function,
            logger=self._logger,
            indexify_server_addr=self._indexify_server_address,
            config_path=self._config_path,
        ).run()
