import queue
import threading
from typing import Any, Iterator, Optional

from indexify.functions_sdk.object_serializer import (
    CloudPickleSerializer,
    get_serializer,
)

from ..proto.function_executor_pb2 import (
    GetInvocationStateRequest,
    InvocationStateRequest,
    InvocationStateResponse,
    SerializedObject,
    SetInvocationStateRequest,
)
from .response_validator import ResponseValidator


class InvocationStateProxyServer:
    """A gRPC server that proxies InvocationState calls to the gRPC client.

    The gRPC client is responsible for the actual implementation of the InvocationState.
    We do the proxying to remove authorization logic and credentials from Function Executor.
    This improves security posture of Function Executor because it may run untrusted code.
    """

    def __init__(
        self, client_responses: Iterator[InvocationStateResponse], logger: Any
    ):
        self._client_responses: Iterator[InvocationStateResponse] = client_responses
        self._logger: Any = logger.bind(module=__name__)
        self._reciever_thread: threading.Thread = threading.Thread(
            target=self._reciever
        )
        self._request_queue: queue.SimpleQueue = queue.SimpleQueue()
        # This lock protects everything below.
        self._lock: threading.Lock = threading.Lock()
        # Python supports big integers natively so we don't need
        # to be worried about interger overflows.
        self._request_seq_num: int = 0
        # Request ID -> Client Response.
        self._response_map: dict[str, InvocationStateResponse] = {}
        self._new_response: threading.Condition = threading.Condition(self._lock)

    def run(self) -> Iterator[InvocationStateRequest]:
        # There's no need to implement shutdown of the server and its threads because
        # the server lives while the Function Executor process lives.
        self._reciever_thread.start()
        yield from self._sender()

    def _reciever(self) -> None:
        self._logger.info("reciever thread started")
        try:
            for response in self._client_responses:
                validator = ResponseValidator(response)
                try:
                    validator.check()
                except ValueError as e:
                    self._logger.error("invalid response from the client", exc_info=e)
                    continue

                with self._lock:
                    self._response_map[response.request_id] = response
                    self._new_response.notify_all()
        except Exception as e:
            self._logger.error("error in reciever thread, exiting", exc_info=e)

    def _sender(self) -> Iterator[InvocationStateRequest]:
        while True:
            yield self._request_queue.get()
            with self._lock:
                # Wait until we get a response for the request.
                # This allows to ensure a serialized order of reads and writes so
                # we can avoid a read returning not previously written value.
                self._new_response.wait()

    def set(self, task_id: str, key: str, value: Any) -> None:
        with self._lock:
            request_id: str = str(self._request_seq_num)
            self._request_seq_num += 1

            # We currently use CloudPickleSerializer for function inputs,
            # outputs and invocation state values. This provides consistent UX.
            request = InvocationStateRequest(
                request_id=request_id,
                task_id=task_id,
                set=SetInvocationStateRequest(
                    key=key,
                    value=SerializedObject(
                        content_type=CloudPickleSerializer.content_type,
                        bytes=CloudPickleSerializer.serialize(value),
                    ),
                ),
            )
            self._request_queue.put(request)
            while request_id not in self._response_map:
                self._new_response.wait()

            response: InvocationStateResponse = self._response_map.pop(request_id)
            if response.request_id != request_id:
                self._logger.error(
                    "response request_id doesn't match actual request_id",
                    request_id=request_id,
                    response=response,
                )
                raise RuntimeError(
                    "response request_id doesn't match actual request_id"
                )
            if not response.HasField("set"):
                self._logger.error(
                    "set response is missing in the client response",
                    request_id=request_id,
                    response=response,
                )
                raise RuntimeError("set response is missing in the client response")
            if not response.success:
                self._logger.error(
                    "failed to set the invocation state for key",
                    key=key,
                )
                raise RuntimeError("failed to set the invocation state for key")

    def get(self, task_id: str, key: str) -> Optional[Any]:
        with self._lock:
            request_id: str = str(self._request_seq_num)
            self._request_seq_num += 1

            request = InvocationStateRequest(
                request_id=request_id,
                task_id=task_id,
                get=GetInvocationStateRequest(
                    key=key,
                ),
            )
            self._request_queue.put(request)
            while request_id not in self._response_map:
                self._new_response.wait()

            response: InvocationStateResponse = self._response_map.pop(request_id)
            if response.request_id != request_id:
                self._logger.error(
                    "response request_id doesn't match actual request_id",
                    request_id=request_id,
                    response=response,
                )
                raise RuntimeError(
                    "response request_id doesn't match actual request_id"
                )
            if not response.HasField("get"):
                self._logger.error(
                    "get response is missing in the client response",
                    request_id=request_id,
                    response=response,
                )
                raise RuntimeError("get response is missing in the client response")
            if not response.success:
                self._logger.error(
                    "failed to get the invocation state for key",
                    key=key,
                )
                raise RuntimeError("failed to get the invocation state for key")
            if not response.get.HasField("value"):
                return None

            return get_serializer(response.get.value.content_type).deserialize(
                response.get.value.bytes
                if response.get.value.HasField("bytes")
                else response.get.value.string
            )
