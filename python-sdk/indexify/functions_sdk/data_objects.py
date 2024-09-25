from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Json


class RouterOutput(BaseModel):
    edges: List[str]


class IndexifyData(BaseModel):
    id: Optional[str] = None
    payload: bytes
    payload_encoding: str = "cbor"


class FunctionWorkerOutput(BaseModel):
    indexify_data: List[IndexifyData]
    exception: Optional[str]
    stdout: Optional[str]
    stderr: Optional[str]
    reducer: bool = False

class File(BaseModel):
    data: bytes
    metadata: Optional[Dict[str, Json]] = None
    sha_256: Optional[str] = None
