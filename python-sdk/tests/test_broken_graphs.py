import sys
import unittest

from indexify import RemoteGraph
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function


@indexify_function()
def extractor_a(url: str) -> File:
    """
    Download pdf from url
    """
    print("`extractor_a` is writing to stdout")
    # print("`extractor_a` is writing to stderr", file=sys.stderr)
    sys.stderr.write(
        "===================== extractor_a is writing to stderr================="
    )
    return File(data="abc", mime_type="application/pdf")


@indexify_function()
def extractor_b(file: File) -> str:
    """
    Download pdf from url
    """
    print("`extractor_b` is writing to stdout", file=sys.stdout)
    print("`extractor_b` is writing to stderr", file=sys.stderr)
    raise Exception("this exception was raised from extractor_b")


@indexify_function()
def extractor_c(s: str) -> str:
    """
    Download pdf from url
    """
    return "this is a return from extractor_c"


def create_broken_graph():
    g = Graph(
        "test-graph-has-an-exception-for-stdout-stderr",
        start_node=extractor_a,
    )

    # Parse the PDF which was downloaded
    g.add_edge(extractor_a, extractor_b)
    g.add_edge(extractor_b, extractor_c)
    return g


class TestBrokenGraphs(unittest.TestCase):
    def test_broken_graph(self):
        g = create_broken_graph()
        g = RemoteGraph.deploy(g)

        self.assertRaises(
            g.run(
                block_until_done=True,
                url="https://www.youtube.com/watch?v=gjHv4pM8WEQ",
            )
        )

        self.assertRaises(
            g.run(
                block_until_done=True,
                maybe="https://www.youtube.com/watch?v=gjHv4pM8WEQ",
            )
        )


if __name__ == "__main__":
    unittest.main()
