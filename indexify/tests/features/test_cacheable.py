import unittest
from collections import defaultdict
from typing import List

from pydantic import BaseModel
from tensorlake import Graph, RemoteGraph, tensorlake_function
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path


class Total(BaseModel):
    val: int = 0


@tensorlake_function(cacheable=True)
def generate_numbers(count: int) -> List[int]:
    return [i + 2 for i in range(count)]


@tensorlake_function(cacheable=True)
def square(x: int) -> int:
    return x**2


@tensorlake_function(accumulate=Total)
def add(total: Total, new: int) -> Total:
    total.val += new
    return total


class TestCacheableGraph(unittest.TestCase):

    def setUp(self):
        graph = Graph(
            name="summation", start_node=generate_numbers, description="Summation"
        )

        graph.add_edge(generate_numbers, square)
        graph.add_edge(square, add)
        self.graph = RemoteGraph.deploy(
            graph, code_dir_path=graph_code_dir_path(__file__)
        )

    def _stream(self) -> tuple[dict[str, int], str]:
        counts = defaultdict(int)
        events = self.graph.stream(block_until_done=True, count=2)
        try:
            while True:
                event = next(events)
                counts[event.event_name] += 1
        except StopIteration as result:
            events.close()
            invocation_id = result.value

        return counts, self.graph.output(invocation_id, "add")

    def test_cached_summation(self):
        counts, result = self._stream()

        self.assertDictEqual(
            {
                "TaskCreated": 7,
                "TaskAssigned": 6,
                "TaskCompleted": 5,
                "InvocationFinished": 1,
            },
            counts,
        )

        self.assertEqual(13, result[0].val)

        counts, result = self._stream()
        self.assertDictEqual(
            {
                "TaskCreated": 2,
                "TaskAssigned": 2,
                "TaskCompleted": 2,
                "TaskMatchedCache": 3,
                "InvocationFinished": 1,
            },
            counts,
        )

        self.assertEqual(13, result[0].val)


if __name__ == "__main__":
    unittest.main()
