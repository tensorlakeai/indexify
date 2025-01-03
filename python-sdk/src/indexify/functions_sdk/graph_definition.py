from typing import Dict, List, Optional

from pydantic import BaseModel

from indexify.functions_sdk.image import ImageInformation

from .object_serializer import get_serializer


class FunctionMetadata(BaseModel):
    name: str
    fn_name: str
    description: str
    reducer: bool = False
    image_information: ImageInformation
    input_encoder: str = "cloudpickle"
    output_encoder: str = "cloudpickle"


class RouterMetadata(BaseModel):
    name: str
    description: str
    source_fn: str
    target_fns: List[str]
    image_information: ImageInformation
    input_encoder: str = "cloudpickle"
    output_encoder: str = "cloudpickle"


class NodeMetadata(BaseModel):
    dynamic_router: Optional[RouterMetadata] = None
    compute_fn: Optional[FunctionMetadata] = None


# RuntimeInformation is a class that holds data about the environment in which the graph should run.
class RuntimeInformation(BaseModel):
    major_version: int
    minor_version: int
    sdk_version: str


class ComputeGraphMetadata(BaseModel):
    name: str
    description: str
    start_node: NodeMetadata
    tags: Dict[str, str] = {}
    nodes: Dict[str, NodeMetadata]
    edges: Dict[str, List[str]]
    accumulator_zero_values: Dict[str, bytes] = {}
    runtime_information: RuntimeInformation
    replaying: bool = False
    version: Optional[int] = -1

    def get_input_payload_serializer(self):
        return get_serializer(self.start_node.compute_fn.input_encoder)

    def get_input_encoder(self) -> str:
        if self.start_node.compute_fn:
            return self.start_node.compute_fn.input_encoder
        elif self.start_node.dynamic_router:
            return self.start_node.dynamic_router.input_encoder

        raise ValueError("start node is not set on the graph")
