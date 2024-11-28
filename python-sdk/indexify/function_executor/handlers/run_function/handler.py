import io
import traceback
from contextlib import redirect_stderr, redirect_stdout

import structlog

from indexify.function_executor.protocol import (
    RunFunctionRequest,
    RunFunctionResponse,
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

logger = structlog.get_logger(module=__name__)


class Handler:
    def __init__(self, request: RunFunctionRequest, indexify_client: IndexifyClient):
        self._request = request
        self._function_loader = FunctionLoader(
            request=request,
            indexify_client=indexify_client,
        )
        self._input_loader = FunctionInputsLoader(request)
        self._response_helper = ResponseHelper(task_id=request.task_id)
        self._func_stdout: io.StringIO = io.StringIO()
        self._func_stderr: io.StringIO = io.StringIO()

    def run(self) -> RunFunctionResponse:
        """Executes the RunFunctionRequest and returns the response.

        Can only be called once per request object.
        Raises an exception if our own code failed, customer function failure doesn't result in any exception.
        Details of customer function failure are returned in the response.
        """
        logger.info(
            "running function",
            task_id=self._request.task_id,
            namespace=self._request.namespace,
            graph_name=self._request.graph_name,
            function_name=self._request.function_name,
            graph_invocation_id=self._request.graph_invocation_id,
        )
        inputs: FunctionInputs = self._input_loader.load()
        return self._run_func_safe_and_captured(inputs)

    def _run_func_safe_and_captured(
        self, inputs: FunctionInputs
    ) -> RunFunctionResponse:
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

    def _run_func(self, inputs: FunctionInputs) -> RunFunctionResponse:
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


def _is_router(func_wrapper: IndexifyFunctionWrapper) -> bool:
    return (
        str(type(func_wrapper.indexify_function))
        == "<class 'indexify.functions_sdk.indexify_functions.IndexifyRouter'>"
    )


def _func_is_reducer(func_wrapper: IndexifyFunctionWrapper) -> bool:
    return func_wrapper.indexify_function.accumulate is not None
