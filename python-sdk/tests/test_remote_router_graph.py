import unittest
from typing import List, Mapping, Union

from pydantic import BaseModel

from indexify import Graph
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.indexify_functions import (
    indexify_function,
    indexify_router,
)
from indexify.local_runner import LocalRunner
from indexify.remote_client import RemoteClient


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
def extractor_y(file: File) -> FileChunk:
    return FileChunk(data=file.data, start=0, end=5)


@indexify_function()
def extractor_z(file: File) -> FileChunk:
    return FileChunk(data=file.data, start=5, end=len(file.data))


@indexify_router()
def router_x(f: File) -> List[Union[extractor_y, extractor_z]]:
    return [extractor_y]


def create_graph_a():
    graph = Graph(
        name="graph_a_router",
        description="description of graph_a",
        start_node=extractor_a,
    )
    graph = graph.add_edge(extractor_a, router_x)
    graph = graph.route(router_x, [extractor_y, extractor_z])
    graph = graph.add_edge(extractor_y, extractor_c)
    graph = graph.add_edge(extractor_z, extractor_c)
    return graph


class TestRemoteClient(unittest.TestCase):
    def test_register_graph(self):
        # Register graph
        client = RemoteClient(namespace="default")
        graph = create_graph_a()
        print(graph.definition().model_dump_json(exclude_none=True))
        client.register_graph(graph)

        # Get graph Defintion
        compute_graph = client.graph("graph_a_router")
        self.assertEqual(compute_graph.name, "graph_a_router")

        # Load and run Graph Code
        graph = client.load_graph("graph_a_router")
        runner = LocalRunner()
        runner.run(graph, url="https://example.com")


if __name__ == "__main__":
    unittest.main()
