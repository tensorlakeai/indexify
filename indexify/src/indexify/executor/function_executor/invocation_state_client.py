import asyncio
from typing import Any, AsyncGenerator, Optional, Union

import grpc
import httpx
from tensorlake.function_executor.proto.function_executor_pb2 import (
    GetInvocationStateResponse,
    InvocationStateRequest,
    InvocationStateResponse,
    SerializedObject,
    SerializedObjectEncoding,
    SetInvocationStateResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.message_validator import MessageValidator

from .metrics.invocation_state_client import (
    metric_request_read_errors,
    metric_server_get_state_request_errors,
    metric_server_get_state_request_latency,
    metric_server_get_state_requests,
    metric_server_set_state_request_errors,
    metric_server_set_state_request_latency,
    metric_server_set_state_requests,
)

# We're currently only supporting CloudPickle for invocation state values.
# FIXME: if Function Executor sends us something else then we fail the calls.
_VALUE_CONTENT_TYPE = "application/octet-stream"


class InvocationStateClient:
    """InvocationStateClient is a client for the invocation state server of a Function Executor.

    The client initializes the Function Executor's invocation state server and executes requests
    it sends to the client.
    """

    def __init__(
        self,
        stub: FunctionExecutorStub,
        base_url: str,
        http_client: httpx.AsyncClient,
        graph: str,
        namespace: str,
        logger: Any,
    ):
        self._stub: FunctionExecutorStub = stub
        self._base_url: str = base_url
        self._http_client: httpx.AsyncClient = http_client
        self._graph: str = graph
        self._namespace: str = namespace
        self._logger: Any = logger.bind(
            module=__name__, graph=graph, namespace=namespace
        )
        self._client_response_queue: asyncio.Queue[
            Union[InvocationStateResponse, str]
        ] = asyncio.Queue()
        self._task_id_to_invocation_id: dict[str, str] = {}
        self._request_loop_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Starts the invocation state client.

        This method initializes the Function Executor's invocation state server first.
        This is why this method needs to be awaited before executing any tasks on the Function Executor
        that might use invocation state feature."""
        server_requests = self._stub.initialize_invocation_state_server(
            self._response_generator()
        )
        self._request_loop_task = asyncio.create_task(
            self._request_loop(server_requests),
            name="graph invocation state client request processing loop",
        )

    def add_task_to_invocation_id_entry(self, task_id: str, invocation_id: str) -> None:
        """Adds a task ID to invocation ID entry to the client's internal state.

        This allows to authorize requests to the invocation state server.
        If a request is not comming from the task ID that was added here then it will
        be rejected. It's caller's responsibility to only add task IDs that are being
        executed by the Function Executor so the Function Executor can't get access to
        invocation state of tasks it doesn't run.

        Doesn't raise any exceptions.
        """
        self._task_id_to_invocation_id[task_id] = invocation_id

    def remove_task_to_invocation_id_entry(self, task_id: str) -> None:
        """Removes a task ID to invocation ID entry from the client's internal state.

        Doesn't raise any exceptions.
        """
        self._task_id_to_invocation_id.pop(task_id, None)

    async def destroy(self) -> None:
        if self._request_loop_task is not None:
            self._request_loop_task.cancel()
        await self._client_response_queue.put("shutdown")

    async def _request_loop(
        self, server_requests: AsyncGenerator[InvocationStateRequest, None]
    ) -> None:
        try:
            async for request in server_requests:
                await self._process_request_no_raise(request)
        except grpc.aio.AioRpcError:
            # Reading from the stream failed.
            # This is a normal situation when the server is shutting down.
            pass
        except asyncio.CancelledError:
            # This async task was cancelled by destroy(). Normal situation too.
            # This exception should not be suppressed, see Python asyncio docs.
            raise
        except Exception as e:
            metric_request_read_errors.inc()
            self._logger.error(
                "failed to read request from server, shutting down invocation state client",
                exc_info=e,
            )

    async def _process_request_no_raise(self, request: InvocationStateRequest) -> None:
        try:
            await self._process_request(request)
        except Exception as e:
            try:
                await self._client_response_queue.put(
                    InvocationStateResponse(
                        request_id=request.request_id,
                        success=False,
                    )
                )
            except Exception as ee:
                self._logger.error("failed to send error response", exc_info=ee)

            self._logger.error(
                "failed to process request",
                exc_info=e,
                request_id=request.request_id,
            )

    async def _process_request(
        self, request: InvocationStateRequest
    ) -> InvocationStateResponse:
        self._validate_request(request)
        # This is a very important check. We don't trust invocation ID and task ID
        # supplied by Function Executor. If a task ID entry doesn't exist then it's
        # a privelege escalation attempt.
        invocation_id: str = self._task_id_to_invocation_id[request.task_id]
        if request.HasField("get"):
            with (
                metric_server_get_state_request_errors.count_exceptions(),
                metric_server_get_state_request_latency.time(),
            ):
                metric_server_get_state_requests.inc()
                value: Optional[SerializedObject] = await self._get_server_state(
                    invocation_id, request.get.key
                )
            await self._client_response_queue.put(
                InvocationStateResponse(
                    request_id=request.request_id,
                    success=True,
                    get=GetInvocationStateResponse(
                        key=request.get.key,
                        value=value,
                    ),
                )
            )
        elif request.HasField("set"):
            with (
                metric_server_set_state_request_errors.count_exceptions(),
                metric_server_set_state_request_latency.time(),
            ):
                metric_server_set_state_requests.inc()
                await self._set_server_state(
                    invocation_id, request.set.key, request.set.value
                )
            await self._client_response_queue.put(
                InvocationStateResponse(
                    request_id=request.request_id,
                    success=True,
                    set=SetInvocationStateResponse(),
                )
            )

    async def _response_generator(
        self,
    ) -> AsyncGenerator[InvocationStateResponse, None]:
        while True:
            response = await self._client_response_queue.get()
            # Hacky cancellation of the generator.
            if response == "shutdown":
                break
            yield response

    async def _set_server_state(
        self, invocation_id: str, key: str, value: SerializedObject
    ) -> None:
        url: str = (
            f"{self._base_url}/internal/namespaces/{self._namespace}/compute_graphs/{self._graph}/invocations/{invocation_id}/ctx/{key}"
        )
        if (
            value.encoding
            != SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE
        ):
            raise ValueError(
                f"Unsupported value encoding: {SerializedObjectEncoding.Name(value.encoding)}. "
                "Only binary pickle is supported for invocation state values."
            )

        response = await self._http_client.post(
            url=url,
            files=[
                (
                    "value",
                    ("value", value.data, _VALUE_CONTENT_TYPE),
                ),
            ],
        )

        try:
            response.raise_for_status()
        except Exception as e:
            self._logger.error(
                "failed to set graph invocation state",
                invocation_id=invocation_id,
                key=key,
                status_code=response.status_code,
                error=response.text,
                exc_info=e,
            )
            raise

    async def _get_server_state(
        self, invocation_id: str, key: str
    ) -> Optional[SerializedObject]:
        url: str = (
            f"{self._base_url}/internal/namespaces/{self._namespace}/compute_graphs/{self._graph}/invocations/{invocation_id}/ctx/{key}"
        )

        response: httpx.Response = await self._http_client.get(url)
        if response.status_code == 404:
            return None

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            self._logger.error(
                f"failed to download graph invocation state value",
                invocation_id=invocation_id,
                key=key,
                status_code=response.status_code,
                error=response.text,
                exc_info=e,
            )
            raise

        return _serialized_object_from_http_response(response)

    def _validate_request(self, request: InvocationStateRequest) -> None:
        (
            MessageValidator(request)
            .required_field("request_id")
            .required_field("task_id")
        )
        if request.HasField("get"):
            (MessageValidator(request.get).required_field("key"))
        elif request.HasField("set"):
            (
                MessageValidator(request.set)
                .required_field("key")
                .required_serialized_object("value")
            )
        else:
            raise ValueError("unknown request type")


def _serialized_object_from_http_response(response: httpx.Response) -> SerializedObject:
    if response.headers["content-type"] != _VALUE_CONTENT_TYPE:
        raise ValueError(
            f"Unexpected content type: {response.headers['content-type']}. "
            f"Expected: {_VALUE_CONTENT_TYPE}."
        )

    return SerializedObject(
        data=response.content,
        encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
        encoding_version=0,
    )
