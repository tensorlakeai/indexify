import tempfile
import unittest
from typing import List, Mapping, Union

import cloudpickle
from pydantic import BaseModel

from indexify import Graph
from indexify.executor.function_worker import FunctionWorker
from indexify.functions_sdk.data_objects import File, IndexifyData
from indexify.functions_sdk.indexify_functions import (
    IndexifyFunctionWrapper,
    indexify_function,
)


@indexify_function()
def extractor_a(url: str) -> File:
    """
    Random description of extractor_a
    """
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


class TestFunctionWorker(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.g = create_graph_a()
        self.function_worker = FunctionWorker()

    async def test_function_worker_happy_path(self):
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            code_bytes = cloudpickle.dumps(self.g.serialize())

            temp_file.write(code_bytes)
            temp_file.flush()
            temp_file_path = temp_file.name

            fn_worker_output = await self.function_worker.async_submit(
                namespace="test",
                graph_name="test",
                fn_name="extractor_b",
                input=IndexifyData(
                    id="123",
                    payload=cloudpickle.dumps(
                        File(data=bytes(b"hello"), mime_type="text/plain")
                    ),
                ),
                code_path=temp_file_path,
                version=1,
            )
            fn_wrapper = IndexifyFunctionWrapper(extractor_b)
            fn_outputs = []
            for worker_output in fn_worker_output.fn_outputs:
                self.assertEqual(worker_output.payload_encoding, "cloudpickle")
                fn_outputs.append(fn_wrapper.deserialize_fn_output(worker_output))
            self.assertEqual(len(fn_outputs), 2)
            expected = FileChunk(data=b"hello", start=5, end=5)

            self.assertEqual(expected, fn_outputs[1])

    async def test_function_worker_extractor_raises_error(self):
        g = create_graph_exception()

        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            code_bytes = cloudpickle.dumps(g.serialize())

            temp_file.write(code_bytes)
            temp_file.flush()
            temp_file_path = temp_file.name

            result = await self.function_worker.async_submit(
                namespace="test",
                graph_name="test",
                fn_name="extractor_exception",
                input=IndexifyData(id="123", payload=cloudpickle.dumps(10)),
                code_path=temp_file_path,
                version=1,
            )
            assert not result.success
            assert result.exception == "this extractor throws an exception."


if __name__ == "__main__":
    unittest.main()
