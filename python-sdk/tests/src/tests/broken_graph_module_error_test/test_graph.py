import io
import unittest
from contextlib import redirect_stdout

from extractors import extractor_a, extractor_c

from indexify import RemoteGraph
from indexify.functions_sdk.graph import Graph


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

        # We don't have a public SDK API to read a function's stderr
        # so we rely on internal SDK behavior where it prints a failed function's
        # stderr to the current stdout.
        func_stdout: io.StringIO = io.StringIO()
        with redirect_stdout(func_stdout):
            g.run(
                block_until_done=True,
                a=10,
            )
        # Use regex because rich formatting characters are present in the output.
        self.assertRegex(func_stdout.getvalue(), r"No module named.*'first_p_dep'")


if __name__ == "__main__":
    unittest.main()
