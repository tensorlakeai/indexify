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
    text_splitter: Literal["char", "recursive"] = "recursive"


class BaseEmbeddingExtractor(Extractor):
    def __init__(self, max_context_length: int):
        self._model_context_length: int = max_context_length

    def extract(
        self, content_list: List[Content], params: EmbeddingInputParams
    ) -> List[List[Content]]:
        splitter: Callable[[str], List[str]] = self._create_splitter(
            params.text_splitter
        )
        extracted_content = []
        for content in content_list:
            extracted_embeddings = []
            if content.content_type != "text/plain":
                continue
            text = content.data.decode("utf-8")
            chunks: List[str] = splitter(text)
            embeddings_list = self.extract_embeddings(chunks)
            for chunk, embeddings in zip(chunks, embeddings_list):
                content = Content.from_text(
                    text=chunk,
                    feature=Feature.embedding(value=embeddings),
                )
                extracted_embeddings.append(content)
            extracted_content.append(extracted_embeddings)
        return extracted_content

    def _create_splitter(self, text_splitter_name: str) -> Callable[[str], List[str]]:
        # TODO Make chunk overlap parameterized
        if text_splitter_name == "recursive":
            return text_splitter.RecursiveCharacterTextSplitter(
                chunk_size=self._model_context_length,
                chunk_overlap=self._model_context_length,
            ).split_text
        elif text_splitter_name == "char":
            return text_splitter.CharacterTextSplitter(
                chunk_size=self._model_context_length, separator="\n\n"
            ).split_text

    @abstractmethod
    def extract_embeddings(self, texts: List[str]) -> List[List[float]]:
        ...