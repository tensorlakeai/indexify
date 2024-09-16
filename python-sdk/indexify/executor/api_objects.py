from typing import Any, Dict, List, Optional

from indexify.functions_sdk.data_objects import BaseData
from pydantic import BaseModel, Json


class Task(BaseModel):
    id: str
    namespace: str
    compute_graph: str
    compute_fn: str
    invocation_id: str
    input_key: str


class ExecutorMetadata(BaseModel):
    id: str
    address: str
    runner_name: str
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
