from typing import List
from indexify_extractor_sdk import (
    ExtractorInfo,
    EmbeddingSchema,
)
from indexify_extractor_sdk.base_embedding import BaseEmbeddingExtractor, EmbeddingInputParams
from indexify_extractor_sdk.sentence_transformer import SentenceTransformersEmbedding

class MiniLML6Extractor(BaseEmbeddingExtractor):
    def __init__(self):
        super(MiniLML6Extractor, self).__init__(max_context_length=128)
        self._model = SentenceTransformersEmbedding(model_name="all-MiniLM-L6-v2")

    def extract_embeddings(self, texts: List[str]) -> List[List[float]]:
        return self._model.embed_ctx(texts)

    def extract_query_embeddings(self, query: str) -> List[float]:
        return self._model.embed_query(query)

    def info(self) -> ExtractorInfo:
        input_params = EmbeddingInputParams()
        return ExtractorInfo(
            name="MiniLML6",
            description="MiniLML6 Embeddings",
            input_params=input_params,
            output_schema=EmbeddingSchema(distance="cosine", dim=384),
        )

