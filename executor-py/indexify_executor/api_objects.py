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


class TaskOutput(BaseModel):
    router: Optional[RouterOutput] = None
    fn: Optional[FnOutput] = None

    @classmethod
    def to_api_object(cls, base_data: Optional[BaseData] = None):
        if base_data is None:
            return None
        output = TaskOutput(fn=FnOutput(payload=base_data.model_dump_json()))
        return output


class TaskResult(BaseModel):
    router_outputs: List[RouterOutput]
    outcome: str
    namespace: str
    compute_graph: str
    compute_fn: str
    invocation_id: str
    executor_id: str
    task_id: str