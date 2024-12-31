import sys
import unittest

from extractors import extractor_a, extractor_c

from indexify import RemoteGraph
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function


def create_broken_graph():
    g = Graph(
        "broken-graph-without-dep-registered",
        start_node=extractor_a,
    )

    # Parse the PDF which was downloaded
    g.add_edge(extractor_a, extractor_c)
    return g


class TestBrokenGraphs(unittest.TestCase):
    def test_broken_graph(self):
        g = create_broken_graph()
        g = RemoteGraph.deploy(g=g)

        self.assertRaises(
            Exception,
            g.run(
                block_until_done=True,
                a=10,
            ),
        )


if __name__ == "__main__":
    unittest.main()
