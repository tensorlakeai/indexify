import unittest
from typing import List, Mapping

from grpc import RpcError
from pydantic import BaseModel
from tensorlake import Graph
from tensorlake.functions_sdk.data_objects import File
from tensorlake.functions_sdk.functions import tensorlake_function
from tensorlake.functions_sdk.object_serializer import CloudPickleSerializer
from testing import (
    DEFAULT_FUNCTION_EXECUTOR_PORT,
    FunctionExecutorProcessContextManager,
    copy_and_modify_request,
    deserialized_function_output,
    rpc_channel,
    run_task,
)

from indexify.function_executor.proto.function_executor_pb2 import (
    InitializeRequest,
    InitializeResponse,
    RunTaskRequest,
    RunTaskResponse,
    SerializedObject,
)
from indexify.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)


@tensorlake_function()
def extractor_a(url: str) -> File:
    print(f"extractor_a called with url: {url}")
    assert url == "https://example.com"
    assert isinstance(url, str)
    return File(data=bytes(b"hello"), mime_type="text/plain")


class FileChunk(BaseModel):
    data: bytes
    start: int
    end: int


@tensorlake_function()
def extractor_b(file: File) -> List[FileChunk]:
    return [
        FileChunk(data=file.data, start=0, end=5),
        FileChunk(data=file.data, start=5, end=len(file.data)),
    ]


class SomeMetadata(BaseModel):
    metadata: Mapping[str, str]


@tensorlake_function()
def extractor_c(file_chunk: FileChunk) -> SomeMetadata:
    return SomeMetadata(metadata={"a": "b", "c": "d"})


@tensorlake_function()
def extractor_exception(a: int) -> int:
    raise Exception("this extractor throws an exception.")


def create_graph_a():
    graph = Graph(name="test", description="test", start_node=extractor_a)
    graph = graph.add_edge(extractor_a, extractor_b)
    graph = graph.add_edge(extractor_b, extractor_c)
    return graph


def create_graph_exception():
    graph = Graph(name="test-exception", description="test", start_node=extractor_a)
    graph = graph.add_edge(extractor_a, extractor_exception)
    graph = graph.add_edge(extractor_exception, extractor_b)
    return graph


class TestRunTask(unittest.TestCase):
    def test_function_success(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = stub.initialize(
                    InitializeRequest(
                        namespace="test",
                        graph_name="test",
                        graph_version="1",
                        function_name="extractor_b",
                        graph=SerializedObject(
                            bytes=CloudPickleSerializer.serialize(
                                create_graph_a().serialize(
                                    additional_modules=[],
                                )
                            ),
                            content_type=CloudPickleSerializer.content_type,
                        ),
                    )
                )
                self.assertTrue(initialize_response.success)

                run_task_response: RunTaskResponse = run_task(
                    stub,
                    function_name="extractor_b",
                    input=File(data=bytes(b"hello"), mime_type="text/plain"),
                )

                self.assertTrue(run_task_response.success)
                self.assertFalse(run_task_response.is_reducer)

                fn_outputs = deserialized_function_output(
                    self, run_task_response.function_output
                )
                self.assertEqual(len(fn_outputs), 2)
                expected = FileChunk(data=b"hello", start=5, end=5)

                self.assertEqual(expected, fn_outputs[1])

    def test_function_raises_error(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 1
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = stub.initialize(
                    InitializeRequest(
                        namespace="test",
                        graph_name="test",
                        graph_version="1",
                        function_name="extractor_exception",
                        graph=SerializedObject(
                            bytes=CloudPickleSerializer.serialize(
                                create_graph_exception().serialize(
                                    additional_modules=[],
                                )
                            ),
                            content_type=CloudPickleSerializer.content_type,
                        ),
                    )
                )
                self.assertTrue(initialize_response.success)

                run_task_response: RunTaskResponse = run_task(
                    stub, function_name="extractor_exception", input=10
                )

                self.assertFalse(run_task_response.success)
                self.assertFalse(run_task_response.is_reducer)
                self.assertTrue(
                    "this extractor throws an exception." in run_task_response.stderr
                )

    def test_wrong_task_routing(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 2
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = stub.initialize(
                    InitializeRequest(
                        namespace="test",
                        graph_name="test",
                        graph_version="1",
                        function_name="extractor_b",
                        graph=SerializedObject(
                            bytes=CloudPickleSerializer.serialize(
                                create_graph_a().serialize(
                                    additional_modules=[],
                                )
                            ),
                            content_type=CloudPickleSerializer.content_type,
                        ),
                    )
                )
                self.assertTrue(initialize_response.success)
                valid_request: RunTaskRequest = RunTaskRequest(
                    namespace="test",
                    graph_name="test",
                    graph_version="1",
                    function_name="extractor_b",
                    graph_invocation_id="123",
                    task_id="test-task",
                    function_input=SerializedObject(
                        bytes=CloudPickleSerializer.serialize(input),
                        content_type=CloudPickleSerializer.content_type,
                    ),
                )
                wrong_requests: List[RunTaskRequest] = [
                    copy_and_modify_request(
                        valid_request, {"namespace": "wrong-namespace"}
                    ),
                    copy_and_modify_request(
                        valid_request, {"graph_name": "wrong-graph-name"}
                    ),
                    copy_and_modify_request(
                        valid_request, {"graph_version": "wrong-graph-version"}
                    ),
                    copy_and_modify_request(
                        valid_request, {"function_name": "wrong-function-name"}
                    ),
                ]
                for request in wrong_requests:
                    self.assertRaises(RpcError, stub.run_task, request)


if __name__ == "__main__":
    unittest.main()
