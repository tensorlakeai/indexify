from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Json


class RouterOutput(BaseModel):
    edges: List[str]


class IndexifyData(BaseModel):
    id: Optional[str] = None
    payload: bytes
    payload_encoding: str = "cbor"


class FunctionWorkerOutput(BaseModel):
    fn_outputs: Optional[List[IndexifyData]]
    router_output: Optional[RouterOutput]
    exception: Optional[str]
    stdout: Optional[str]
    stderr: Optional[str]
    reducer: bool = False


class File(BaseModel):
    data: bytes
    metadata: Optional[Dict[str, Json]] = None
    sha_256: Optional[str] = None
