from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class DataPayload(BaseModel):
    path: str
    size: int
    sha256_hash: str
    content_type: Optional[str] = None


class NodeGPU(BaseModel):
    count: int
    model: str


class TaskResources(BaseModel):
    cpus: float
    memory_mb: int
    ephemeral_disk_mb: int
    gpu: Optional[NodeGPU] = None


class TaskRetryPolicy(BaseModel):
    max_retries: int
    initial_delay_sec: float
    max_delay_sec: float
    delay_multiplier: float


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
    graph_payload: Optional[DataPayload] = None
    input_payload: Optional[DataPayload] = None
    reducer_input_payload: Optional[DataPayload] = None
    output_payload_uri_prefix: Optional[str] = None
    timeout: Optional[int] = None  # in seconds
    resources: Optional[TaskResources] = None
    retry_policy: Optional[TaskRetryPolicy] = None


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


class IngestFnOutputsResponse(BaseModel):
    data_payloads: List[DataPayload]
    stdout: Optional[DataPayload] = None
    stderr: Optional[DataPayload] = None


TASK_OUTCOME_SUCCESS = "success"
TASK_OUTCOME_FAILURE = "failure"
