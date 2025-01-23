import io
import sys
import traceback
from contextlib import redirect_stderr, redirect_stdout
from typing import Any

from tensorlake.functions_sdk.functions import (
    FunctionCallResult,
    GraphInvocationContext,
    RouterCallResult,
    TensorlakeFunctionWrapper,
    TensorlakeRouter,
)
from tensorlake.functions_sdk.invocation_state.invocation_state import InvocationState

from ...proto.function_executor_pb2 import RunTaskRequest, RunTaskResponse
from .function_inputs_loader import FunctionInputs, FunctionInputsLoader
from .response_helper import ResponseHelper


class Handler:
    def __init__(
        self,
        request: RunTaskRequest,
        graph_name: str,
        graph_version: str,
        function_name: str,
        invocation_state: InvocationState,
        function_wrapper: TensorlakeFunctionWrapper,
        logger: Any,
    ):
        self._invocation_id: str = request.graph_invocation_id
        self._graph_name: str = graph_name
        self._graph_version: str = graph_version
        self._function_name: str = function_name
        self._invocation_state: InvocationState = invocation_state
        self._logger = logger.bind(
            graph_invocation_id=request.graph_invocation_id,
            task_id=request.task_id,
        )
        self._function_wrapper = function_wrapper
        self._input_loader = FunctionInputsLoader(request)
        self._response_helper = ResponseHelper(task_id=request.task_id)
        # TODO: use files for stdout, stderr capturing. This puts a natural and thus reasonable
        # rate limit on the rate of writes and allows to not consume expensive memory for function logs.
        self._func_stdout: io.StringIO = io.StringIO()
        self._func_stderr: io.StringIO = io.StringIO()

    def run(self) -> RunTaskResponse:
        """Runs the task.

        Raises an exception if our own code failed, customer function failure doesn't result in any exception.
        Details of customer function failure are returned in the response.
        """
        self._logger.info("running function")
        inputs: FunctionInputs = self._input_loader.load()
        self._flush_logs()
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
        ctx: GraphInvocationContext = GraphInvocationContext(
            invocation_id=self._invocation_id,
            graph_name=self._graph_name,
            graph_version=self._graph_version,
            invocation_state=self._invocation_state,
        )
        if _is_router(self._function_wrapper):
            result: RouterCallResult = self._function_wrapper.invoke_router(
                ctx, self._function_name, inputs.input
            )
            return self._response_helper.router_response(
                result=result,
                stdout=self._func_stdout.getvalue(),
                stderr=self._func_stderr.getvalue(),
            )
        else:
            result: FunctionCallResult = self._function_wrapper.invoke_fn_ser(
                ctx, self._function_name, inputs.input, inputs.init_value
            )
            return self._response_helper.function_response(
                result=result,
                is_reducer=_function_is_reducer(self._function_wrapper),
                stdout=self._func_stdout.getvalue(),
                stderr=self._func_stderr.getvalue(),
            )

    def _flush_logs(self) -> None:
        # Flush any logs buffered in memory before running the function with stdout, stderr capture.
        # Otherwise our logs logged before this point will end up in the function's stdout.
        # structlog.PrintLogger uses print function. This is why flushing with print works.
        print("", flush=True)
        sys.stdout.flush()
        sys.stderr.flush()


def _is_router(func_wrapper: TensorlakeFunctionWrapper) -> bool:
    """Determines if the function is a router.

    A function is a router if it is an instance of TensorlakeRouter or if it is an TensorlakeRouter class.
    """
    return str(
        type(func_wrapper.indexify_function)
    ) == "<class 'tensorlake.functions_sdk.functions.TensorlakeRouter'>" or isinstance(
        func_wrapper.indexify_function, TensorlakeRouter
    )


def _function_is_reducer(func_wrapper: TensorlakeFunctionWrapper) -> bool:
    return func_wrapper.indexify_function.accumulate is not None
