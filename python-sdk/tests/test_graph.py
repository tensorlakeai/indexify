import unittest
from typing import List, Mapping, Union

from pydantic import BaseModel

from indexify import Graph, create_client, LocalClient
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.indexify_functions import (
    indexify_function,
    indexify_router,
)


class YoutubeURL(BaseModel):
    url: str


@indexify_function()
def extractor_a_with_pydantic_model_as_input(url: YoutubeURL) -> File:
    return File(data=bytes(b"hello"), mime_type="text/plain")


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


@indexify_function()
def extractor_exception(file: File) -> SomeMetadata:
    raise Exception("This executor will raise an exception.")


@indexify_router()
def router_x(input: File) -> List[Union[extractor_b, extractor_d]]:
    return [extractor_d]


def create_graph_a():
    graph = Graph(name="test", description="test", start_node=extractor_a)
    graph = graph.add_edge(extractor_a, extractor_b)
    graph = graph.add_edge(extractor_b, extractor_c)
    return graph


def create_graph_b():
    graph = Graph(
        name="test",
        description="test",
        start_node=extractor_a_with_pydantic_model_as_input,
    )
    graph = graph.add_edge(extractor_a_with_pydantic_model_as_input, router_x)
    graph = graph.route(router_x, [extractor_b, extractor_d])
    return graph


def create_graph_c():
    graph = Graph(name="test", description="test", start_node=extractor_a)
    graph = graph.add_edge(extractor_a, extractor_exception)
    graph = graph.add_edge(extractor_exception, extractor_c)

    return graph


class TestGraphA(unittest.TestCase):
    def test_deserialize_input_from_dict(self):
        graph = create_graph_a()
        input_data = graph.deserialize_input_from_dict(
            extractor_a.name, {"url": "https://example.com"}
        )
        self.assertEqual(input_data, {"url": "https://example.com"})

    def test_deserialize_input_from_json_with_pydantic_model(self):
        graph = create_graph_a()
        input_data = graph.deserialize_input_from_dict(
            extractor_b.name, {"data": b"hello", "sha_256": "123"}
        )
        self.assertEqual(input_data, File(data=bytes(b"hello"), sha_256="123"))

    def test_deserialize_input_from_json_with_named_param_and_pydantic_model_value(
        self,
    ):
        graph = create_graph_b()
        input_data = graph.deserialize_input_from_dict(
            extractor_a_with_pydantic_model_as_input.name,
            {"url": {"url": "https://example.com"}},
        )
        self.assertEqual(input_data, YoutubeURL(url="https://example.com"))

    def test_run_graph(self):
        graph = create_graph_a()

        runner = create_client(local=True)
        runner.run(graph, url="https://example.com")

    def test_run_graph_with_pydantic_model_as_input(self):
        graph = Graph(
            name="test",
            description="test",
            start_node=extractor_a_with_pydantic_model_as_input,
        )
        graph = graph.add_edge(extractor_a_with_pydantic_model_as_input, extractor_b)
        graph = graph.add_edge(extractor_b, extractor_c)

        runner = create_client(local=True)
        runner.register_compute_graph(graph)
        runner.invoke_graph_with_object(
            graph.name, url=YoutubeURL(url="https://example.com")
        )

    def test_run_graph_with_router(self):
        graph = create_graph_b()

        runner = create_client(local=True)
        runner.register_compute_graph(graph)
        runner.invoke_graph_with_object(
            graph.name, url=YoutubeURL(url="https://example.com")
        )

    def test_run_graph_with_executor_exception(self):
        graph = create_graph_c()

        runner = create_client(local=True)
        runner.register_compute_graph(graph)

        try:
            runner.invoke_graph_with_object(
                graph.name, url="https://example.com"
            )
        except Exception as e:
            self.assertEqual(str(e), "This executor will raise an exception.")

    def test_get_graph_metadata_with_router(self):
        graph = create_graph_b()
        metadata = graph.definition()
        expected_graph_metadata = """{
    "name": "test",
    "description": "test",
    "start_node": {
        "compute_fn": {
            "name": "extractor_a_with_pydantic_model_as_input",
            "fn_name": "extractor_a_with_pydantic_model_as_input",
            "description": ""
        }
    },
    "nodes": {
        "extractor_a_with_pydantic_model_as_input": {
            "compute_fn": {
                "name": "extractor_a_with_pydantic_model_as_input",
                "fn_name": "extractor_a_with_pydantic_model_as_input",
                "description": ""
            }
        },
        "router_x": {
            "dynamic_router": {
                "name": "router_x",
                "description": "",
                "source_fn": "router_x",
                "target_fns": [
                    "extractor_b",
                    "extractor_d"
                ]
            }
        },
        "extractor_b": {
            "compute_fn": {
                "name": "extractor_b",
                "fn_name": "extractor_b",
                "description": ""
            }
        },
        "extractor_d": {
            "compute_fn": {
                "name": "extractor_d",
                "fn_name": "extractor_d",
                "description": ""
            }
        }
    },
    "edges": {
        "extractor_a_with_pydantic_model_as_input": [
            "router_x"
        ]
    },
    "accumulator_zero_values": {}
}"""
        result = metadata.model_dump_json(indent=4, exclude_none=True)
        self.maxDiff = None
        self.assertEqual(result, expected_graph_metadata)

    def test_get_graph_metadata(self):
        graph = create_graph_a()
        metadata = graph.definition()
        expected_graph_metadata = """{
    "name": "test",
    "description": "test",
    "start_node": {
        "compute_fn": {
            "name": "extractor_a",
            "fn_name": "extractor_a",
            "description": "Random description of extractor_a"
        }
    },
    "nodes": {
        "extractor_a": {
            "compute_fn": {
                "name": "extractor_a",
                "fn_name": "extractor_a",
                "description": "Random description of extractor_a"
            }
        },
        "extractor_b": {
            "compute_fn": {
                "name": "extractor_b",
                "fn_name": "extractor_b",
                "description": ""
            }
        },
        "extractor_c": {
            "compute_fn": {
                "name": "extractor_c",
                "fn_name": "extractor_c",
                "description": ""
            }
        }
    },
    "edges": {
        "extractor_a": [
            "extractor_b"
        ],
        "extractor_b": [
            "extractor_c"
        ]
    },
    "accumulator_zero_values": {}
}"""
        result = metadata.model_dump_json(indent=4, exclude_none=True)
        self.assertEqual(result, expected_graph_metadata)


if __name__ == "__main__":
    unittest.main()
