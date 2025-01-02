import sys
import unittest
from typing import List, Mapping

from pydantic import BaseModel

from indexify import Graph
from indexify.function_executor.proto.function_executor_pb2 import (
    InitializeRequest,
    InitializeResponse,
    RunTaskResponse,
    SerializedObject,
)
from indexify.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.indexify_functions import indexify_function
from indexify.functions_sdk.object_serializer import CloudPickleSerializer
from tests.function_executor.utils import (
    FunctionExecutorServerTestCase,
    deserialized_function_output,
    run_task,
)


@indexify_function()
def extractor_a(url: str) -> File:
    print(f"extractor_a called with url: {url}")
    assert url == "https://example.com"
    assert isinstance(url, str)
    return File(data=bytes(b"hello"), mime_type="text/plain")


class FileChunk(BaseModel):
    data: bytes
    start: int
    end: int


@indexify_function()
def extractor_b(file: File) -> List[FileChunk]:
    return [
        FileChunk(data=file.data, start=0, end=5),
        FileChunk(data=file.data, start=5, end=len(file.data)),
    ]


class SomeMetadata(BaseModel):
    metadata: Mapping[str, str]


@indexify_function()
def extractor_c(file_chunk: FileChunk) -> SomeMetadata:
    return SomeMetadata(metadata={"a": "b", "c": "d"})


@indexify_function()
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


class TestRunTask(FunctionExecutorServerTestCase):
    def test_function_success(self):
        with self._rpc_channel() as channel:
            stub: FunctionExecutorStub = FunctionExecutorStub(channel)
            initialize_response: InitializeResponse = stub.initialize(
                InitializeRequest(
                    namespace="test",
                    graph_name="test",
                    graph_version=1,
                    function_name="extractor_b",
                    graph=SerializedObject(
                        bytes=CloudPickleSerializer.serialize(
                            create_graph_a().serialize(
                                additional_modules=[sys.modules[__name__]]
                            )
                        ),
                        content_type=CloudPickleSerializer.content_type,
                    ),
                )
            )
            self.assertTrue(initialize_response.success)

            run_task_response: RunTaskResponse = run_task(
                stub, File(data=bytes(b"hello"), mime_type="text/plain")
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
        with self._rpc_channel() as channel:
            stub: FunctionExecutorStub = FunctionExecutorStub(channel)
            initialize_response: InitializeResponse = stub.initialize(
                InitializeRequest(
                    namespace="test",
                    graph_name="test",
                    graph_version=1,
                    function_name="extractor_exception",
                    graph=SerializedObject(
                        bytes=CloudPickleSerializer.serialize(
                            create_graph_exception().serialize(
                                additional_modules=[sys.modules[__name__]]
                            )
                        ),
                        content_type=CloudPickleSerializer.content_type,
                    ),
                )
            )
            self.assertTrue(initialize_response.success)

            run_task_response: RunTaskResponse = run_task(stub, 10)

            self.assertFalse(run_task_response.success)
            self.assertFalse(run_task_response.is_reducer)
            self.assertTrue(
                "this extractor throws an exception." in run_task_response.stderr
            )


if __name__ == "__main__":
    unittest.main()
