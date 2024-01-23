from abc import abstractmethod
import json
from typing import Any, Callable, List, Literal
from pydantic import BaseModel
from langchain import text_splitter

from .base_extractor import (
    Content,
    Extractor,
    Feature,
)


class EmbeddingInputParams(BaseModel):
    overlap: int = 0
    chunk_size: int = 0
    text_splitter: Literal["char", "recursive"] = "recursive"


class BaseEmbeddingExtractor(Extractor):
    input_mimes = ["text/plain"]
    def __init__(self, max_context_length: int):
        self._model_context_length: int = max_context_length

    def extract(
        self, content: Content, params: EmbeddingInputParams
    ) -> List[Content]:
        if params.chunk_size == 0:
            params.chunk_size = self._model_context_length

        splitter: Callable[[str], List[str]] = self._create_splitter(params)
        extracted_embeddings = []
        text = content.data.decode("utf-8")
        chunks: List[str] = splitter(text)
        embeddings_list = self.extract_embeddings(chunks)
        for chunk, embeddings in zip(chunks, embeddings_list):
            content = Content.from_text(
                text=chunk,
                feature=Feature.embedding(values=embeddings),
            )
            extracted_embeddings.append(content)
        return extracted_embeddings

    def _create_splitter(
        self, input_params: EmbeddingInputParams
    ) -> Callable[[str], List[str]]:
        if input_params.text_splitter == "recursive":
            return text_splitter.RecursiveCharacterTextSplitter(
                chunk_size=input_params.chunk_size,
                chunk_overlap=input_params.overlap,
            ).split_text
        elif input_params.text_splitter == "char":
            return text_splitter.CharacterTextSplitter(
                chunk_size=input_params.chunk_size,
                chunk_overlap=input_params.overlap,
                separator="\n\n",
            ).split_text

    @abstractmethod
    def extract_embeddings(self, texts: List[str]) -> List[List[float]]:
        ...

    def sample_input(self) -> Content:
        return Content.from_text("hello world")
