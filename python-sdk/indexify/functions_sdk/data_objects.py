from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Json


class RouterOutput(BaseModel):
    edges: List[str]


class IndexifyData(BaseModel):
    id: Optional[str] = None
    payload: bytes
    payload_encoding: str = "cbor"


class File(BaseModel):
    data: bytes
    metadata: Optional[Dict[str, Json]] = None
    sha_256: Optional[str] = None
