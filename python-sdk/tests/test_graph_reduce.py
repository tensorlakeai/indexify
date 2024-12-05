import unittest
from typing import List

from parameterized import parameterized
from pydantic import BaseModel

from indexify import Graph
from indexify.functions_sdk.indexify_functions import indexify_function
from tests.testing import remote_or_local_graph, test_graph_name


class TestGraphReduce(unittest.TestCase):
    @parameterized.expand([(True), (False)])
    def test_simple(self, is_remote):
        class AccumulatedSate(BaseModel):
            sum: int = 19

        @indexify_function()
        def generate_seq(x: int) -> List[int]:
            return [i for i in range(x)]

        @indexify_function(accumulate=AccumulatedSate)
        def accumulate_reduce(acc: AccumulatedSate, y: int) -> AccumulatedSate:
            acc.sum += y
            return acc

        @indexify_function()
        def store_result(acc: AccumulatedSate) -> int:
            return acc.sum

        graph = Graph(name=test_graph_name(self), start_node=generate_seq)
        graph.add_edge(generate_seq, accumulate_reduce)
        graph.add_edge(accumulate_reduce, store_result)

        metadata = graph.definition()
        metadata_json = metadata.model_dump(exclude_none=True)
        print(metadata_json)

        graph = remote_or_local_graph(graph, remote=is_remote)
        invocation_id = graph.run(block_until_done=True, x=3)
        result = graph.output(invocation_id, store_result.name)
        self.assertEqual(result[0], 22)

    @parameterized.expand([(True)])
    def test_failure_in_parent(self, is_remote: bool):
        class AccumulatedSate(BaseModel):
            sum: int = 0

        @indexify_function()
        def generate_seq(x: int) -> List[int]:
            return [i for i in range(x)]

        @indexify_function()
        def add_one(x: int) -> int:
            if x == 0:
                raise ValueError("test: add_one function failed")
            return x + 1

        @indexify_function(accumulate=AccumulatedSate)
        def accumulate_reduce(acc: AccumulatedSate, y: int) -> AccumulatedSate:
            acc.sum += y
            return acc

        @indexify_function()
        def store_result(acc: AccumulatedSate) -> int:
            return acc.sum

        graph = Graph(
            name=test_graph_name(self),
            start_node=generate_seq,
        )
        graph.add_edge(generate_seq, add_one)
        graph.add_edge(add_one, accumulate_reduce)
        graph.add_edge(accumulate_reduce, store_result)
        graph = remote_or_local_graph(graph, remote=is_remote)

        invocation_id = graph.run(block_until_done=True, x=3)
        result = graph.output(invocation_id, store_result.name)
        self.assertEqual(len(result), 0)

    @parameterized.expand([(True)])
    def test_failure_start_node(self, is_remote: bool):
        class AccumulatedSate(BaseModel):
            sum: int = 0

        @indexify_function()
        def generate_seq(x: int) -> List[int]:
            raise ValueError("test: generate_seq function failed")
            # return [i for i in range(x)]

        @indexify_function()
        def add_one(x: int) -> int:
            return x + 1

        @indexify_function(accumulate=AccumulatedSate)
        def accumulate_reduce(acc: AccumulatedSate, y: int) -> AccumulatedSate:
            acc.sum += y
            return acc

        @indexify_function()
        def store_result(acc: AccumulatedSate) -> int:
            return acc.sum

        graph = Graph(
            name=test_graph_name(self),
            start_node=generate_seq,
        )
        graph.add_edge(generate_seq, add_one)
        graph.add_edge(add_one, accumulate_reduce)
        graph.add_edge(accumulate_reduce, store_result)
        graph = remote_or_local_graph(graph, remote=is_remote)

        invocation_id = graph.run(block_until_done=True, x=3)
        outputs = graph.output(invocation_id, store_result.name)
        self.assertEqual(len(outputs), 0, "Expected zero results")


if __name__ == "__main__":
    unittest.main()
