from typing import List

import httpx
import time
from pydantic import BaseModel

from indexify import create_client
from indexify.functions_sdk.data_objects import File


class Object(BaseModel):
    x: str


from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function


@indexify_function()
def update(x: Object) -> Object:
    return Object(x=x.x + "b")


@indexify_function()
def update2(x: Object) -> Object:
    return Object(x=x.x + "c")


if __name__ == "__main__":
    g = Graph(name="updategraph1", start_node=update)
    client = create_client()
    client.register_compute_graph(g)
    invocation_id = client.invoke_graph_with_object(
        g.name,
        block_until_done=True,
        x=Object(x="a"),
    )
    output = client.graph_outputs(g.name, invocation_id, fn_name="update")
    print(output)

    g = Graph(name="updategraph1", start_node=update2)
    client.register_compute_graph(g)
    client.rerun_graph(g.name)
    time.sleep(1)
    output = client.graph_outputs(g.name, invocation_id, fn_name="update2")
    print(output)
