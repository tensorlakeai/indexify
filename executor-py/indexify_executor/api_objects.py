from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Json


class Task(BaseModel):
    id: str
    namespace: str
    compute_graph: str
    compute_fn: str
    invocation_id: str
    input_id: str


class ExecutorMetadata(BaseModel):
    id: str
    address: str
    runner_name: str
    labels: Dict[str, Any]


class RouterOutput(BaseModel):
    edges: List[str]


class FnOutput(BaseModel):
    router: Json 

class TaskOutput(BaseModel):
    router: Optional[RouterOutput]
    fn: Optional[FnOutput]


class TaskResult(BaseModel):
    outputs: List[TaskOutput]
    outcome: str
    namespace: str
    compute_graph: str
    compute_fn: str
    invocation_id: str
    executor_id: str