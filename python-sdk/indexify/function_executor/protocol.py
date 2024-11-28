from typing import List, Optional

from pydantic import BaseModel

# This is a temporary request-response protocol between Executor and Function Executor.
# It'll be used until we migrate to a proper RPC protocol between the two.
#
# All objects are serialized using jsonpickle Python module and then UTF-8 encoded.
# The objects used in the messages should not use any Python SDK objects because only
# Function Executor implemented in Python is allowed to import Python SDK to run customer
# functions. This ensures that all the other components can be written in any language
# once we migrate off jsonpickle and pydantic serialization here.


class BinaryData(BaseModel):
    """Stores a single BLOB of data.

    Passing blobs allows to hide details of Python SDK objects from Executor and Server.
    """

    data: bytes
    content_type: str


class RunFunctionRequest(BaseModel):
    """Request for run function command."""

    namespace: str
    graph_name: str
    graph_version: int
    graph_invocation_id: str
    function_name: str
    task_id: str
    graph: BinaryData
    # Graph invocation payload is the input data for the first function in the graph.
    graph_invocation_payload: Optional[BinaryData]
    # Function input is the output of the previous function in the graph. None for the first function.
    function_input: Optional[BinaryData]
    # Reducer init value. None for non-reducers.
    function_init_value: Optional[BinaryData]


class FunctionOutput(BaseModel):
    outputs: List[BinaryData]


class RouterOutput(BaseModel):
    edges: List[str]


class RunFunctionResponse(BaseModel):
    """Response for run function command."""

    task_id: str
    function_output: Optional[FunctionOutput]
    router_output: Optional[RouterOutput]
    stdout: Optional[str]
    stderr: Optional[str]
    is_reducer: bool
    success: bool
