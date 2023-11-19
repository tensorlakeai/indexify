import cv2
from pathlib import Path
from matplotlib import pyplot as plt
from IPython.core.display import HTML

import deepdoctection as dd
import numpy as np
from typing import List
from indexify_extractor_sdk import (
    ExtractorInfo,
    EmbeddingSchema,
)
from indexify_extractor_sdk.base_embedding import BaseEmbeddingExtractor, EmbeddingInputParams
from indexify_extractor_sdk.sentence_transformer import SentenceTransformersEmbedding

"""
    Implements a Hash Extractor, which can be used to find duplicates within the dataset.
    It hashes the text into bytes, and interprets these are a numpy array.

    We can extend this by LocalitySensitiveHashing, to also account for small perturbations in the input bytes.

    This is equivalent to an identity mapping (with the sample-size n large enough, there will be collisions, but this is highly unlikely )
"""
class PdfRawTextExtractor(BaseEmbeddingExtractor):

    def __init__(self, max_context_length: int = 512, language = 'en'):
        super(PdfRawTextExtractor, self).__init__(max_context_length=512)
        self._model = dd.get_dd_analyzer(config_overwrite=[f"LANGUAGE='{language}'"])

    def extract_embeddings(self, texts: List[str]) -> List[List[float]]:
        # TODO: Wait to merge new Content and FeatureContent
        return [self._embed(text) for text in texts]

    def extract_query_embeddings(self, query: str) -> List[float]:
        return self._embed(query)

    def info(self) -> ExtractorInfo:
        input_params = EmbeddingInputParams()
        return ExtractorInfo(
            name="PdfRawTextExtractor",
            description="PdfRawTextExtractor",
            input_params=input_params,
            output_schema=EmbeddingSchema(distance="cosine", dim=32),
        )

    # Should return both
    def _embed(self, text) -> List[float]:
        self._model.update(bytes(text, 'utf-8'))
        bytearray = self._model.digest()
        return np.frombuffer(bytearray, dtype=np.int8).tolist()
