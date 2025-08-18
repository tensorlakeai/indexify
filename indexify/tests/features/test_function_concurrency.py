import time
import unittest
from typing import List

from tensorlake import (
    Graph,
    tensorlake_function,
)
from testing import remote_or_local_graph, test_graph_name, wait_function_output

MAX_CONCURRENCY = 10
concurrency_counter: int = 0


@tensorlake_function(max_concurrency=MAX_CONCURRENCY)
def concurrent_function() -> int:
    global concurrency_counter
    concurrency_counter += 1
    observed_max_concurrency = concurrency_counter
    # Simulate long IO bound work
    time.sleep(10)
    concurrency_counter -= 1
    return observed_max_concurrency


class TestFunctionConcurrency(unittest.TestCase):
    def test_function_reaches_max_concurrency(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=concurrent_function,
        )
        graph = remote_or_local_graph(graph, remote=True)
        invocation_ids: List[str] = []
        for _ in range(MAX_CONCURRENCY):
            invocation_id = graph.run(block_until_done=False)
            invocation_ids.append(invocation_id)

        observed_max_concurrencies: List[int] = []
        for invocation_id in invocation_ids:
            outputs = wait_function_output(
                graph, invocation_id, concurrent_function.name
            )
            self.assertEqual(len(outputs), 1)
            observed_max_concurrencies.append(outputs[0])

        self.assertEqual(
            set(observed_max_concurrencies), set(range(1, MAX_CONCURRENCY + 1))
        )


if __name__ == "__main__":
    unittest.main()
