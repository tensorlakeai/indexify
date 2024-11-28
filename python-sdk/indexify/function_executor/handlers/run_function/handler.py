import io
import traceback
from contextlib import redirect_stderr, redirect_stdout
from typing import Optional

import structlog

from indexify.function_executor.proto.function_executor_pb2 import (
    RunTaskRequest,
    RunTaskResponse,
)
from indexify.functions_sdk.indexify_functions import (
    FunctionCallResult,
    IndexifyFunctionWrapper,
    RouterCallResult,
)
from indexify.http_client import IndexifyClient

from .function_inputs_loader import FunctionInputs, FunctionInputsLoader
from .function_loader import FunctionLoader
from .response_helper import ResponseHelper


class Handler:
    def __init__(
        self,
        request: RunTaskRequest,
        indexify_server_addr: str,
        config_path: Optional[str],
    ):
        self._request = request
        self._logger = structlog.get_logger(module=__name__).bind(
            task_id=request.task_id
        )
        self._function_loader = FunctionLoader(
            request=request,
            indexify_client=_indexify_client(
                logger=self._logger,
                request=request,
                indexify_server_addr=indexify_server_addr,
                config_path=config_path,
            ),
        )
        self._input_loader = FunctionInputsLoader(request)
        self._response_helper = ResponseHelper(task_id=request.task_id)
        self._func_stdout: io.StringIO = io.StringIO()
        self._func_stderr: io.StringIO = io.StringIO()

    def run(self) -> RunTaskResponse:
        """Executes the RunFunctionRequest and returns the response.

        Can only be called once.
        Raises an exception if our own code failed, customer function failure doesn't result in any exception.
        Details of customer function failure are returned in the response.
        """
        self._logger.info(
            "running function",
            task_id=self._request.task_id,
            namespace=self._request.namespace,
            graph_name=self._request.graph_name,
            function_name=self._request.function_name,
            graph_invocation_id=self._request.graph_invocation_id,
        )
        inputs: FunctionInputs = self._input_loader.load()
        return self._run_func_safe_and_captured(inputs)

    def _run_func_safe_and_captured(self, inputs: FunctionInputs) -> RunTaskResponse:
        """Runs the customer function while capturing what happened in it.

        Function stdout and stderr are captured so they don't get into Function Executor process stdout
        and stderr. Never throws an Exception. Caller can determine if the function succeeded
        using the response.
        """
        try:
            with redirect_stdout(self._func_stdout), redirect_stderr(self._func_stderr):
                return self._run_func(inputs)
        except Exception:
            return self._response_helper.failure_response(
                message=traceback.format_exc(),
                stdout=self._func_stdout.getvalue(),
                stderr=self._func_stderr.getvalue(),
            )

    def _run_func(self, inputs: FunctionInputs) -> RunTaskResponse:
        func_wrapper: IndexifyFunctionWrapper = self._function_loader.load()
        if _is_router(func_wrapper):
            result: RouterCallResult = func_wrapper.invoke_router(
                self._request.function_name, inputs.input
            )
            return self._response_helper.router_response(
                result=result,
                stdout=self._func_stdout.getvalue(),
                stderr=self._func_stderr.getvalue(),
            )
        else:
            result: FunctionCallResult = func_wrapper.invoke_fn_ser(
                self._request.function_name, inputs.input, inputs.init_value
            )
            return self._response_helper.function_response(
                result=result,
                is_reducer=_func_is_reducer(func_wrapper),
                stdout=self._func_stdout.getvalue(),
                stderr=self._func_stderr.getvalue(),
            )


def _indexify_client(
    logger,
    request: RunTaskRequest,
    indexify_server_addr: str,
    config_path: Optional[str],
) -> IndexifyClient:
    # This client is required to implement key/value store functionality for customer functions.
    protocol: str = "http"
    if config_path:
        logger.info("TLS is enabled")
        protocol = "https"
    return IndexifyClient(
        service_url=f"{protocol}://{indexify_server_addr}",
        namespace=request.namespace,
        config_path=config_path,
    )


def _is_router(func_wrapper: IndexifyFunctionWrapper) -> bool:
    return (
        str(type(func_wrapper.indexify_function))
        == "<class 'indexify.functions_sdk.indexify_functions.IndexifyRouter'>"
    )


def _func_is_reducer(func_wrapper: IndexifyFunctionWrapper) -> bool:
    return func_wrapper.indexify_function.accumulate is not None
