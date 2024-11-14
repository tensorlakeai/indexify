from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Json


class FileInput(BaseModel):
    url: str
    mime_type: Optional[str] = None
    metadata: Optional[Dict[str, Json]] = None
    sha_256: Optional[str] = None


class RouterOutput(BaseModel):
    edges: List[str]


class IndexifyData(BaseModel):
    id: Optional[str] = None
    payload: Union[bytes, str]
    encoder: Literal["cloudpickle", "json"] = "cloudpickle"


class FunctionWorkerOutput(BaseModel):
    fn_outputs: Optional[List[IndexifyData]]
    router_output: Optional[RouterOutput]
    stdout: Optional[str]
    stderr: Optional[str]
    reducer: bool = False
    success: bool = True


class File(BaseModel):
    data: bytes
    mime_type: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    sha_256: Optional[str] = None
