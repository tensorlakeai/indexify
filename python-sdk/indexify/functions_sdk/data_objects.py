from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Json


class RouterOutput(BaseModel):
    edges: List[str]


class BaseData(BaseModel):
    content_id: Optional[str] = None
    payload: Optional[Any] = None


class File(BaseModel):
    data: bytes
    metadata: Optional[Dict[str, Json]] = None
    sha_256: Optional[str] = None
