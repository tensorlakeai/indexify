from typing import Iterator, Optional

import grpc
import structlog
from tensorlake.functions_sdk.functions import TensorlakeFunctionWrapper
from tensorlake.functions_sdk.object_serializer import get_serializer

from .handlers.run_function.handler import Handler as RunTaskHandler
from .handlers.run_function.request_validator import (
    RequestValidator as RunTaskRequestValidator,
)
from .info import info_response_kv_args
from .initialize_request_validator import InitializeRequestValidator
from .invocation_state.invocation_state_proxy_server import InvocationStateProxyServer
from .invocation_state.proxied_invocation_state import ProxiedInvocationState
from .proto.function_executor_pb2 import (
    HealthCheckRequest,
    HealthCheckResponse,
    InfoRequest,
    InfoResponse,
    InitializeRequest,
    InitializeResponse,
    InvocationStateResponse,
    RunTaskRequest,
    RunTaskResponse,
)
from .proto.function_executor_pb2_grpc import FunctionExecutorServicer


class Service(FunctionExecutorServicer):
    def __init__(self):
        self._logger = structlog.get_logger(module=__name__).bind(
            **info_response_kv_args()
        )
        self._namespace: Optional[str] = None
        self._graph_name: Optional[str] = None
        self._graph_version: Optional[str] = None
        self._function_name: Optional[str] = None
        self._function_wrapper: Optional[TensorlakeFunctionWrapper] = None
        self._invocation_state_proxy_server: Optional[InvocationStateProxyServer] = None

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
        self._logger = self._logger.bind(
            namespace=request.namespace,
            graph_name=request.graph_name,
            graph_version=request.graph_version,
            function_name=request.function_name,
        )
        graph_serializer = get_serializer(request.graph.content_type)
        try:
            # Process user controlled input in a try-except block to not treat errors here as our
            # internal platform errors.
            # TODO: capture stdout and stderr and report exceptions the same way as when we run a task.
            graph = graph_serializer.deserialize(request.graph.bytes)
            function = graph_serializer.deserialize(graph[request.function_name])
            self._function_wrapper = TensorlakeFunctionWrapper(function)
        except Exception as e:
            return InitializeResponse(success=False, customer_error=str(e))

        self._logger.info("initialized function executor service")
        return InitializeResponse(success=True)

    def initialize_invocation_state_server(
        self,
        client_responses: Iterator[InvocationStateResponse],
        context: grpc.ServicerContext,
    ):
        self._invocation_state_proxy_server = InvocationStateProxyServer(
            client_responses, self._logger
        )
        self._logger.info("initialized invocation proxy server")
        yield from self._invocation_state_proxy_server.run()

    def run_task(
        self, request: RunTaskRequest, context: grpc.ServicerContext
    ) -> RunTaskResponse:
        # Customer function code never raises an exception because we catch all of them and add
        # their details to the response. We can only get an exception here if our own code failed.
        # If our code raises an exception the grpc framework converts it into GRPC_STATUS_UNKNOWN
        # error with the exception message. Differentiating errors is not needed for now.
        RunTaskRequestValidator(request=request).check()
        self._check_task_routed_correctly(request)

        return RunTaskHandler(
            request=request,
            graph_name=self._graph_name,
            graph_version=self._graph_version,
            function_name=self._function_name,
            invocation_state=ProxiedInvocationState(
                request.task_id, self._invocation_state_proxy_server
            ),
            function_wrapper=self._function_wrapper,
            logger=self._logger,
        ).run()

    def _check_task_routed_correctly(self, request: RunTaskRequest):
        # Fail with internal error as this happened due to wrong task routing to this Server.
        # If we run the wrongly routed task then it can steal data from this Server if it belongs
        # to a different customer.
        if request.namespace != self._namespace:
            raise ValueError(
                f"This Function Executor is not initialized for this namespace {request.namespace}"
            )
        if request.graph_name != self._graph_name:
            raise ValueError(
                f"This Function Executor is not initialized for this graph_name {request.graph_name}"
            )
        if request.graph_version != self._graph_version:
            raise ValueError(
                f"This Function Executor is not initialized for this graph_version {request.graph_version}"
            )
        if request.function_name != self._function_name:
            raise ValueError(
                f"This Function Executor is not initialized for this function_name {request.function_name}"
            )

    def check_health(
        self, request: HealthCheckRequest, context: grpc.ServicerContext
    ) -> HealthCheckResponse:
        # This health check validates that the Server:
        # - Has its process alive (not exited).
        # - Didn't exhaust its thread pool.
        # - Is able to communicate over its server socket.
        return HealthCheckResponse(healthy=True)

    def get_info(
        self, request: InfoRequest, context: grpc.ServicerContext
    ) -> InfoResponse:
        return InfoResponse(**info_response_kv_args())
