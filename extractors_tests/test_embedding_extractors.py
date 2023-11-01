import unittest
import json
from minilm_l6_embedding import (
    MiniLML6Extractor,
)
from parameterized import parameterized
from typing import Type
from indexify_extractor_sdk.base_extractor import Extractor, Content, ExtractorSchema
from indexify_extractor_sdk.base_embedding import EmbeddingInputParams


class TestMiniLML6Extractor(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestMiniLML6Extractor, self).__init__(*args, **kwargs)

    @parameterized.expand(
        [("minilm6", MiniLML6Extractor())]
    )
    def test_ctx_embeddings(self, extractor_name: str, extractor: Type[Extractor]):
        embeddings = extractor.extract([Content(id="1", content_type="text", data="hello world")], EmbeddingInputParams())
        embeddings_values = json.loads(embeddings[0].feature.value)
        self.assertEqual(len(embeddings), 1)
        self.assertEqual(embeddings[0].feature.feature_type, "embedding")
        self.assertEqual(len(embeddings_values), 384)

    @parameterized.expand(
        [("minilm6", MiniLML6Extractor())]
    )
    def test_query_embeddings(self, extractor_name: str, extractor: Type[Extractor]):
        embeddings = extractor.extract_query_embeddings("hello world")
        self.assertEqual(len(embeddings), 384)

    @parameterized.expand(
        [("minilm6", MiniLML6Extractor())]
    )
    def test_extractor_info(self, extractor_name: str, extractor: Type[Extractor]):
        schema: ExtractorSchema = extractor.schemas()
        self.assertIsNotNone(schema.model_dump()["embedding_schemas"]["embedding"])


if __name__ == "__main__":
    unittest.main()
