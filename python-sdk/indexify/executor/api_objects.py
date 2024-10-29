from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Json

from indexify.functions_sdk.data_objects import IndexifyData


class Task(BaseModel):
    id: str
    namespace: str
    compute_graph: str
    compute_fn: str
    invocation_id: str
    input_key: str
    reducer_output_id: Optional[str] = None
    graph_version: int


class ExecutorMetadata(BaseModel):
    id: str
    executor_version: str
    addr: str
    image_name: str
    image_version: int
    labels: Dict[str, Any]


class RouterOutput(BaseModel):
    edges: List[str]


class FnOutput(BaseModel):
    payload: Json


class TaskResult(BaseModel):
    router_output: Optional[RouterOutput] = None
    outcome: str
    namespace: str
    compute_graph: str
    compute_fn: str
    invocation_id: str
    executor_id: str
    task_id: str
    reducer: bool = False
