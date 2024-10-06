from typing import List, Optional, Any

from pydantic import BaseModel


class TextChunk(BaseModel):
    chunk: str
    page_number: Optional[int] = None
    embeddings: Optional[List[float]] = None


class ImageWithEmbedding(BaseModel):
    embedding: List[float]
    page_number: int

class DocumentImage(BaseModel):
    page_number: int
    # This is so that we don't have to import PIL.Image at the top of the file
    image: Any

class DocumentImages(BaseModel):
    images: List[DocumentImage]