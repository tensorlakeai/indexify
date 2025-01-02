from typing import List, Optional, Any

from pydantic import BaseModel


class TextChunk(BaseModel):
    chunk: str
    page_number: Optional[int] = None
    embeddings: Optional[List[float]] = None


class ImageWithEmbedding(BaseModel):
    embedding: List[float]
    image_bytes: bytes
    page_number: int


# Docling Example Objects
class PDFParserDoclingOutput(BaseModel):
    texts: List[str]
    images: List[str]