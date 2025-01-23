import subprocess
import unittest
from typing import Any, Dict, List, Optional

import grpc
from tensorlake.functions_sdk.object_serializer import CloudPickleSerializer

from indexify.function_executor.proto.function_executor_pb2 import (
    FunctionOutput,
    RunTaskRequest,
    RunTaskResponse,
    SerializedObject,
)
from indexify.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from indexify.function_executor.proto.server_configuration import GRPC_SERVER_OPTIONS

# Default Executor range is 50000:51000.
# Use a value outside of this range to not conflict with other tests.
DEFAULT_FUNCTION_EXECUTOR_PORT: int = 60000


class FunctionExecutorProcessContextManager:
    def __init__(self, port: int = DEFAULT_FUNCTION_EXECUTOR_PORT):
        self._args = [
            "function-executor",
            "--dev",
            "--address",
            f"localhost:{port}",
        ]
        self._process: Optional[subprocess.Popen] = None
        self.port = port

    def __enter__(self) -> "FunctionExecutorProcessContextManager":
        self._process = subprocess.Popen(self._args)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._process:
            self._process.terminate()
            self._process.wait()


def rpc_channel(context_manager: FunctionExecutorProcessContextManager) -> grpc.Channel:
    # The GRPC_SERVER_OPTIONS include the maximum message size which we need to set in the client channel.
    channel: grpc.Channel = grpc.insecure_channel(
        f"localhost:{context_manager.port}",
        options=GRPC_SERVER_OPTIONS,
    )
    try:
        SERVER_STARTUP_TIMEOUT_SEC = 5
        # This is not asyncio.Future but grpc.Future. It has a different interface.
        grpc.channel_ready_future(channel).result(timeout=SERVER_STARTUP_TIMEOUT_SEC)
        return channel
    except Exception as e:
        channel.close()
        raise Exception(
            f"Failed to connect to the gRPC server within {SERVER_STARTUP_TIMEOUT_SEC} seconds"
        ) from e


def run_task(
    stub: FunctionExecutorStub, function_name: str, input: Any, **kwargs
) -> RunTaskResponse:
    return stub.run_task(
        RunTaskRequest(
            namespace="test",
            graph_name="test",
            graph_version="1",
            function_name=function_name,
            graph_invocation_id="123",
            task_id="test-task",
            function_input=SerializedObject(
                bytes=CloudPickleSerializer.serialize(input),
                content_type=CloudPickleSerializer.content_type,
            ),
        ),
        **kwargs,
    )


def deserialized_function_output(
    test_case: unittest.TestCase, function_output: FunctionOutput
) -> List[Any]:
    outputs: List[Any] = []
    for output in function_output.outputs:
        test_case.assertEqual(output.content_type, CloudPickleSerializer.content_type)
        outputs.append(CloudPickleSerializer.deserialize(output.bytes))
    return outputs


def copy_and_modify_request(
    src: RunTaskRequest, modifications: Dict[str, Any]
) -> RunTaskRequest:
    request = RunTaskRequest()
    request.CopyFrom(src)
    for key, value in modifications.items():
        setattr(request, key, value)
    return request
