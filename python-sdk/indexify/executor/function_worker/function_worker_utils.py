import os
from functools import cache

import cloudpickle

from indexify import IndexifyClient
from indexify.functions_sdk.indexify_functions import GraphInvocationContext, IndexifyFunctionWrapper

@cache
def _load_function(
    namespace: str,
    graph_name: str,
    fn_name: str,
    code_path: str,
    version: int,
    invocation_id: str,
    indexify_client: IndexifyClient,
):
    """Load an extractor to the memory: extractor_wrapper_map."""
    with open(code_path, "rb") as f:
        code = f.read()
        pickled_functions = cloudpickle.loads(code)
    context = GraphInvocationContext(
        invocation_id=invocation_id,
        graph_name=graph_name,
        graph_version=str(version),
        indexify_client=indexify_client,
    )
    return IndexifyFunctionWrapper(
        cloudpickle.loads(pickled_functions[fn_name]),
        context,
    )
