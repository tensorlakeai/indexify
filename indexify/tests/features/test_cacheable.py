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
        events = self.graph.stream(count=2)
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

        # NB: We use a constant here specifically to make it obvious
        # that we're spelling the key exactly the same way in each
        # check; since we're using a defaultdict, it would be easy to
        # get the spelling wrong and have the first check successfully
        # assert the count is == 0.
        match_key = "TaskMatchedCache"

        self.assertEqual(13, result[0].val)
        self.assertEqual(0, counts[match_key])

        counts, result = self._stream()

        self.assertEqual(13, result[0].val)
        self.assertEqual(3, counts[match_key])

        print(f"Cached counts: {counts}")


if __name__ == "__main__":
    unittest.main()
