from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class Task(BaseModel):
    id: str
    namespace: str
    compute_graph: str
    compute_fn: str
    invocation_id: str
    input_key: str
    reducer_output_id: Optional[str] = None
    graph_version: str
    image_uri: Optional[str] = None
    "image_uri defines the URI of the image of this task. Optional since some executors do not require it."
    secret_names: Optional[List[str]] = None
    "secret_names defines the names of the secrets to set on function executor. Optional for backward compatibility."


class FunctionURI(BaseModel):
    namespace: str
    compute_graph: str
    compute_fn: str
    version: Optional[str] = None


class ExecutorMetadata(BaseModel):
    id: str
    executor_version: str
    addr: str
    function_allowlist: Optional[List[FunctionURI]] = None
    labels: Dict[str, Any]


class RouterOutput(BaseModel):
    edges: List[str]


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


TASK_OUTCOME_SUCCESS = "success"
TASK_OUTCOME_FAILURE = "failure"
