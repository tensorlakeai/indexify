import unittest

from indexify import Graph
from indexify.executor.function_worker import FunctionWorker
from indexify.functions_sdk.indexify_functions import indexify_function, indexify_router
from indexify.functions_sdk.data_objects import File
from pydantic import BaseModel
from typing import List, Union, Mapping
import tempfile
from indexify.functions_sdk.data_objects import IndexifyData
import cbor2

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


class SomeOtherFileTransform(BaseModel):
    data: bytes


@indexify_function()
def extractor_d(file: File) -> SomeOtherFileTransform:
    return SomeOtherFileTransform(data=file.data)


class SomeMetadata(BaseModel):
    metadata: Mapping[str, str]


@indexify_function()
def extractor_c(file_chunk: FileChunk) -> SomeMetadata:
    return SomeMetadata(metadata={"a": "b", "c": "d"})


@indexify_router()
def router_x(input: File) -> List[Union[extractor_b, extractor_d]]:
    return [extractor_d]


def create_graph_a():
    graph = Graph(name="test", description="test", start_node=extractor_a)
    graph = graph.add_edge(extractor_a, extractor_b)
    graph = graph.add_edge(extractor_b, extractor_c)
    return graph


class TestFunctionWorker(unittest.IsolatedAsyncioTestCase):
    async def test_function_worker(self):
        g = create_graph_a()
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            code_bytes = g.serialize()
            temp_file.write(code_bytes)
            temp_file.flush()
            temp_file_path = temp_file.name
            function_worker = FunctionWorker()
            await function_worker.async_submit(
                namespace="test",
                graph_name="test",
                fn_name="extractor_b",
                input=IndexifyData(
                    id="123",
                    payload=cbor2.dumps(
                        File(data=bytes(b"hello"), mime_type="text/plain").model_dump()
                    ),
                ),
                code_path=temp_file_path,
            )


if __name__ == "__main__":
    unittest.main()