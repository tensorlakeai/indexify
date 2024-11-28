import os
import sys
import tempfile
import unittest
from typing import List, Mapping

import jsonpickle
from pydantic import BaseModel

from indexify import Graph
from indexify.executor.paths.host import HostPaths
from indexify.function_executor.protocol import (
    BinaryData,
    RunFunctionRequest,
    RunFunctionResponse,
)
from indexify.function_executor.server import Server as FunctionExecutorServer
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.indexify_functions import indexify_function
from indexify.functions_sdk.object_serializer import CloudPickleSerializer
from indexify.http_client import IndexifyClient


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


class TestFunctionExecutorServer(unittest.TestCase):
    def setUp(self):
        pass

    def _write_request(self, request: RunFunctionRequest):
        serialized_request = jsonpickle.encode(request).encode("utf-8")
        request_path = HostPaths.task_run_function_request(request.task_id)
        os.makedirs(os.path.dirname(request_path), exist_ok=True)
        with open(request_path, "wb") as f:
            f.write(serialized_request)

    def _read_response(self, task_id: str) -> RunFunctionResponse:
        with open(HostPaths.task_run_function_response(task_id), "rb") as f:
            return jsonpickle.decode(f.read().decode("utf-8"))

    def test_function_executor_success(self):
        with tempfile.TemporaryDirectory() as tmp_dir_path:
            task_id = "test-task"
            HostPaths.set_base_dir(tmp_dir_path)

            self._write_request(
                RunFunctionRequest(
                    namespace="test",
                    graph_name="test",
                    graph_version=1,
                    graph_invocation_id="123",
                    function_name="extractor_b",
                    task_id=task_id,
                    graph=BinaryData(
                        data=CloudPickleSerializer.serialize(
                            create_graph_a().serialize(
                                additional_modules=[sys.modules[__name__]]
                            )
                        ),
                        content_type=CloudPickleSerializer.content_type,
                    ),
                    graph_invocation_payload=None,
                    function_input=BinaryData(
                        data=CloudPickleSerializer.serialize(
                            File(data=bytes(b"hello"), mime_type="text/plain")
                        ),
                        content_type=CloudPickleSerializer.content_type,
                    ),
                    function_init_value=None,
                )
            )

            FunctionExecutorServer(
                task_id=task_id, indexify_client=IndexifyClient()
            ).run()
            response: RunFunctionResponse = self._read_response(task_id)

            self.assertTrue(response.success)
            self.assertFalse(response.is_reducer)

            fn_outputs = []
            for output in response.function_output.outputs:
                self.assertEqual(
                    output.content_type, CloudPickleSerializer.content_type
                )
                fn_outputs.append(CloudPickleSerializer.deserialize(output.data))
            self.assertEqual(len(fn_outputs), 2)
            expected = FileChunk(data=b"hello", start=5, end=5)

            self.assertEqual(expected, fn_outputs[1])

    def test_function_executor_extractor_raises_error(self):
        with tempfile.TemporaryDirectory() as tmp_dir_path:
            task_id = "test-task"
            HostPaths.set_base_dir(tmp_dir_path)

            self._write_request(
                RunFunctionRequest(
                    namespace="test",
                    graph_name="test",
                    graph_version=1,
                    graph_invocation_id="123",
                    function_name="extractor_exception",
                    task_id=task_id,
                    graph=BinaryData(
                        data=CloudPickleSerializer.serialize(
                            create_graph_exception().serialize(
                                additional_modules=[sys.modules[__name__]]
                            )
                        ),
                        content_type=CloudPickleSerializer.content_type,
                    ),
                    graph_invocation_payload=None,
                    function_input=BinaryData(
                        data=CloudPickleSerializer.serialize(10),
                        content_type=CloudPickleSerializer.content_type,
                    ),
                    function_init_value=None,
                )
            )

            FunctionExecutorServer(
                task_id=task_id, indexify_client=IndexifyClient()
            ).run()
            response: RunFunctionResponse = self._read_response(task_id)

            self.assertFalse(response.success)
            self.assertFalse(response.is_reducer)
            self.assertTrue("this extractor throws an exception." in response.stderr)


if __name__ == "__main__":
    unittest.main()
