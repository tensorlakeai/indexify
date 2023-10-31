from typing import List

# alias to avoid name conflict with FlagEmbedding class below
from fastembed.embedding import FlagEmbedding as FastFlagEmbedding

from indexify_extractor_sdk import (
    ExtractorInfo,
    EmbeddingSchema,
)

from indexify_extractor_sdk.base_embedding import BaseEmbeddingExtractor, EmbeddingInputParams


class FlagEmbedding(BaseEmbeddingExtractor):
    def __init__(self, max_length: int = 512):
        super(FlagEmbedding, self).__init__(max_context_length=max_length)
        self.embedding_model = FastFlagEmbedding(
            model_name="BAAI/bge-small-en", max_length=max_length
        )

    def extract_embeddings(self, texts: List[str]) -> List[List[float]]:
        return [next(self.embedding_model.passage_embed(texts)).tolist()]

    def extract_query_embeddings(self, query: str) -> List[float]:
        return list(next(self.embedding_model.query_embed([query])))

    def info(self) -> ExtractorInfo:
        input_params = EmbeddingInputParams()
        return ExtractorInfo(
            name="bge-small-en",
            description="Flag Embeddings",
            input_params=input_params,
            output_schema=EmbeddingSchema(distance="cosine", dim=384),
        )
