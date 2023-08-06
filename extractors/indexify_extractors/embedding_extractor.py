from abc import ABC, abstractmethod

from typing import Any, Optional, List, Literal

from dataclasses import dataclass
from pydantic import BaseModel
from enum import Enum
from .sentence_transformer import SentenceTransformersEmbedding
from .extractor_base import Datatype, Extractor, ExtractorInfo, Content

class EmbeddingInputParams(BaseModel):
    overlap: int = 0
    text_splitter: Literal['char', 'token', 'recursive', 'new_line']  = 'new_line'

@dataclass
class ExtractedEmbedding:
    content_id: str
    text: str
    embeddings: List[float]


@dataclass
class EmbeddingSchema:
    distance_metric: str
    dim: int


class MiniLML6Extractor(Extractor):
    def __init__(self):
        self._model = SentenceTransformersEmbedding(model_name="all-MiniLM-L6-v2")

    def extract(self, content_list: List[Content], params: dict[str, Any]) -> List[ExtractedEmbedding]:
        extracted_embeddings = []
        for content in content_list:
            chunks = self.create_chunks(content.data, 0, "")
            embeddings_list = self._model.embed_ctx(chunks)
            for (chunk, embeddings) in zip(chunks, embeddings_list):
                extracted_embeddings.append(ExtractedEmbedding(content_id=content.id, text=chunk, embeddings=embeddings))
            
        return extracted_embeddings
    
    def extract_query_embeddings(self, query: str) -> List[float]:
        return self._model.embed_query(query)

    def info(self) -> ExtractorInfo:
        return ExtractorInfo(
            name="MiniLML6",
            description="MiniLML6 Embeddings",
            output_datatype="embedding",
            output_schema=EmbeddingSchema(distance_metric="cosine", dim=384),
        )
    
    def create_chunks(self, text: str, overlap: int, text_splitter: str) -> List[str]:
        return [text]