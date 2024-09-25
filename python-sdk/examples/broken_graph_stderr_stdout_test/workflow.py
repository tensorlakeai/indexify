import sys

from indexify import create_client
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import (
    indexify_function,
)


@indexify_function()
def extractor_a(url: str) -> File:
    """
    Download pdf from url
    """
    print("`extractor_a` is writing to stdout")
    # print("`extractor_a` is writing to stderr", file=sys.stderr)
    sys.stderr.write('===================== extractor_a is writing to stderr=================')
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


if __name__ == "__main__":
    g = Graph(
        "test-graph-has-an-exception-for-stdout-stderr",
        start_node=extractor_a,
    )

    # Parse the PDF which was downloaded
    g.add_edge(extractor_a, extractor_b)
    g.add_edge(extractor_b, extractor_c)

    client = create_client()
    client.register_compute_graph(g)
    invocation_id = client.invoke_graph_with_object(
        g.name, block_until_done=True, url="https://www.youtube.com/watch?v=gjHv4pM8WEQ",
    )

    # invocation_id = "4bd41e4e8a694c66"
    # print(f"[bold] Retrieving transcription for {invocation_id} [/bold]")
    # outputs = client.graph_outputs(
    #     g.name, invocation_id=invocation_id, fn_name=extractor_c.name
    # )
