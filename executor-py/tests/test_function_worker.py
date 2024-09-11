import unittest
from typing import List, Mapping
from unittest import IsolatedAsyncioTestCase
from pydantic import BaseModel
import os

from indexify import Graph
from indexify.functions_sdk.data_objects import BaseData, File
from indexify.functions_sdk.indexify_functions import indexify_function
from indexify_extractor_sdk.function_worker import FunctionWorker


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


def create_graph_a():
    graph = Graph(name="test", description="test", start_node=extractor_a)
    graph = graph.add_edge(extractor_a, extractor_b)
    graph = graph.add_edge(extractor_b, extractor_c)
    return graph


class TestFunctionWorker(IsolatedAsyncioTestCase):
    def __init__(self, *args, **kwargs):
        super(TestFunctionWorker, self).__init__(*args, **kwargs)

    async def test_run_graph(self):
        graph = create_graph_a()
        code =graph.serialize()
        code_path = "~/.indexify/graphs/default/test.pickle"
        os.makedirs(os.path.dirname(code_path), exist_ok=True)
        with open(code_path, "wb") as f:
            f.write(code)
        worker = FunctionWorker()
        out = await worker.async_submit("default", "test", "extractor_a", {"url": "https://example.com"}, code_path)
        self.assertEqual(out[0].payload.data, b"hello")


if __name__ == "__main__":
    unittest.main()