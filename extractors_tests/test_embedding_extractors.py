import unittest
from indexify_extractors.embedding_extractor import (
    MiniLML6Extractor,
    Content,
    FlagEmbedding,
    ExtractorInfo,
)
from parameterized import parameterized
from typing import Type
from indexify_extractors.base_extractor import Extractor


class TestMiniLML6Extractor(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestMiniLML6Extractor, self).__init__(*args, **kwargs)

    @parameterized.expand(
        [("minilm6", MiniLML6Extractor()), ("flag-embedding", FlagEmbedding())]
    )
    def test_ctx_embeddings(self, extractor_name: str, extractor: Type[Extractor]):
        embeddings = extractor.extract([Content(id="1", data="hello world")], {})
        self.assertEqual(len(embeddings), 1)
        self.assertEqual(len(embeddings[0].embeddings), 384)

    @parameterized.expand(
        [("minilm6", MiniLML6Extractor()), ("flag-embedding", FlagEmbedding())]
    )
    def test_query_embeddings(self, extractor_name: str, extractor: Type[Extractor]):
        embeddings = extractor.extract_query_embeddings("hello world")
        self.assertEqual(len(embeddings), 384)

    @parameterized.expand(
        [("minilm6", MiniLML6Extractor()), ("flag-embedding", FlagEmbedding())]
    )
    def test_extractor_info(self, extractor_name: str, extractor: Type[Extractor]):
        extractor_info: ExtractorInfo = extractor.info()
        self.assertIsNotNone(extractor_info.model_dump()["output_schema"]["embedding"])


if __name__ == "__main__":
    unittest.main()
