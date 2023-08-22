from abc import ABC, abstractmethod

from typing import Any, Optional, List, Literal, Callable, Union

from dataclasses import dataclass
from pydantic import BaseModel

from .extractor_base import Content
from .sentence_transformer import SentenceTransformersEmbedding
from .extractor_base import Datatype, Extractor, ExtractorInfo, Content, ExtractedEmbedding
import langchain

class EmbeddingInputParams(BaseModel):
    overlap: int = 0
    text_splitter: Literal['char', 'recursive']  = 'recursive'

@dataclass
class EmbeddingSchema:
    distance_metric: str
    dim: int

class BaseEmbeddingExtractor(Extractor):

    def __init__(self, max_context_length: int):
        self._model_context_length: int = max_context_length

    def extract(self, content_list: List[Content], params: dict[str, Any]) -> List[Any]:
        input_params: EmbeddingInputParams = EmbeddingInputParams.parse_obj(params)
        text_splitter: Any[[str], List[str]] = self._create_splitter(input_params.text_splitter)
        extracted_embeddings = []
        for content in content_list:
            chunks: List[str] = text_splitter(content.data)
            embeddings_list = self.extract_embeddings(chunks)
            for (chunk, embeddings) in zip(chunks, embeddings_list):
                extracted_embeddings.append(ExtractedEmbedding(content_id=content.id, text=chunk, embeddings=embeddings))
        return extracted_embeddings
        
    def _create_splitter(self, text_splitter: str) -> Callable[[str], List[str]]:
        if text_splitter == "recursive":
            return langchain.text_splitter.RecursiveCharacterTextSplitter(chunk_size=self._model_context_length).split_text
        elif text_splitter == "char":
            return langchain.text_splitter.CharacterTextSplitter(chunk_size=self._model_context_length, separator="\n\n").split_text

    @abstractmethod        
    def extract_embeddings(self, texts: List[str]) -> List[List[float]]:
        ...


class FlagEmbedding(BaseEmbeddingExtractor):
    def __init__(self):
        self.embedding_model = FastFlagEmbedding(model_name="BAAI/bge-small-en", max_length=512)
    

    def extract_embeddings(self, texts: List[str]) -> List[List[float]]:
        # prepend "passage:" to each text
        texts = ["passage: " + text for text in texts]
        return list(self.embedding_model.encode(texts))

    def extract_query_embeddings(self, query: str) -> List[float]:
        # prepend "query:" to query
        query = "query: " + query
        return list(self.embedding_model.encode([query])[0])

    def info(self) -> ExtractorInfo:
        return ExtractorInfo(
            name="bge-small-en",
            description="Flag Embeddings",
            output_datatype="embedding",
            input_params=EmbeddingInputParams.schema_json(),
            output_schema=EmbeddingSchema(distance_metric="cosine", dim=384),
        )   

class MiniLML6Extractor(BaseEmbeddingExtractor):

    def __init__(self):
        super(MiniLML6Extractor, self).__init__(max_context_length=384)
        self._model = SentenceTransformersEmbedding(model_name="all-MiniLM-L6-v2")

    def extract_embeddings(self, texts: List[str]) -> List[List[float]]:
        return self._model.embed_ctx(texts)

    def extract_query_embeddings(self, query: str) -> List[float]:
        return self._model.embed_query(query)

    def info(self) -> ExtractorInfo:
        return ExtractorInfo(
            name="MiniLML6",
            description="MiniLML6 Embeddings",
            output_datatype="embedding",
            input_params=EmbeddingInputParams.schema_json(),
            output_schema=EmbeddingSchema(distance_metric="cosine", dim=384),
        )        