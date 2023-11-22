import hashlib
import numpy as np
from typing import List

from indexify_extractor_sdk import (ExtractorSchema, EmbeddingSchema)
from indexify_extractor_sdk.base_embedding import (BaseEmbeddingExtractor, EmbeddingInputParams)

class IdentityHashEmbedding(BaseEmbeddingExtractor):
    """
        Implements a Hash Extractor, which can be used to find duplicates within the dataset.
        It hashes the text into bytes, and interprets these are a numpy array.

        We can extend this by LocalitySensitiveHashing, to also account for small perturbations in the input bytes.

        This is equivalent to an identity mapping (with the sample-size n large enough, there will be collisions, but this is highly unlikely )
    """

    def __init__(self):
        super(IdentityHashEmbedding, self).__init__(max_context_length=128)

    def extract_embeddings(self, texts: List[str]) -> List[List[float]]:
        return [self._embed(text) for text in texts]

    def extract_query_embeddings(self, query: str) -> List[float]:
        return self._embed(query)

    def schemas(self) -> ExtractorSchema:
        input_params = EmbeddingInputParams()
        return ExtractorSchema(
            input_params=input_params.model_dump_json(),
            embedding_schemas={
                "embedding": EmbeddingSchema(distance_metric="cosine", dim=32)
            },
        )

    def _embed(self, text) -> List[float]:
        model = hashlib.sha256()
        model.update(bytes(text, 'utf-8'))
        out = model.digest()
        return np.frombuffer(out, dtype=np.int8).tolist()
