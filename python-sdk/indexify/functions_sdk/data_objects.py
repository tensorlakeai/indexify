from typing import Any, Dict, List, Optional, Union

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
    payload: bytes
    payload_encoding: str = "cloudpickle"


class FunctionWorkerOutput(BaseModel):
    fn_outputs: Optional[List[IndexifyData]]
    router_output: Optional[RouterOutput]
    exception: Optional[str]
    stdout: Optional[str]
    stderr: Optional[str]
    reducer: bool = False
    success: bool = True


class File(BaseModel):
    data: bytes
    mime_type: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    sha_256: Optional[str] = None
