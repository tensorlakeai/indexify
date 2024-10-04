from typing import List, Optional

from pydantic import BaseModel


class TextChunk(BaseModel):
    chunk: str
    page_number: Optional[int] = None
    embeddings: Optional[List[float]] = None


class ImageWithEmbedding(BaseModel):
    embedding: List[float]
    page_number: int
