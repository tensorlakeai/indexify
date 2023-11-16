from typing import List
from indexify_extractor_sdk import (
    ExtractorSchema,
    EmbeddingSchema,
    Content,
)
from indexify_extractor_sdk.base_embedding import (
    BaseEmbeddingExtractor,
    EmbeddingInputParams,
)
from indexify_extractor_sdk.sentence_transformer import SentenceTransformersEmbedding


class MiniLML6Extractor(BaseEmbeddingExtractor):
    def __init__(self):
        super(MiniLML6Extractor, self).__init__(max_context_length=128)
        self._model = SentenceTransformersEmbedding(model_name="all-MiniLM-L6-v2")

    def extract_embeddings(self, texts: List[str]) -> List[List[float]]:
        return self._model.embed_ctx(texts)

    def extract_query_embeddings(self, query: str) -> List[float]:
        return self._model.embed_query(query)

    def schemas(self) -> ExtractorSchema:
        input_params = EmbeddingInputParams()
        return ExtractorSchema(
            input_params=input_params.model_dump_json(),
            embedding_schemas={
                "embedding": EmbeddingSchema(distance_metric="cosine", dim=384)
            },
        )


if __name__ == "__main__":
    extractor = MiniLML6Extractor()
    print(extractor.schemas())
    print(extractor.extract_query_embeddings("Hello World"))
    print(extractor.extract([Content.from_text(text="Hello World")], {}))