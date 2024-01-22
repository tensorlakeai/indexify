from .base_extractor import Extractor, Content, Feature, EmbeddingSchema, ExtractorSchema

from typing import List

from pydantic import BaseModel

import json

class InputParams(BaseModel):
    a: int = 0
    b: str = ""

class MockExtractor(Extractor):
    def __init__(self):
        super().__init__()

    def extract(
        self, content: List[Content], params: InputParams
    ) -> List[List[Content]]:
        return [
            [
                Content.from_text(
                    text="Hello World", feature=Feature.embedding(value=[1, 2, 3])
                ),
                Content.from_text(
                    text="Pipe Baz", feature=Feature.embedding(value=[1, 2, 3])
                ),
                Content.from_text(
                    text="Hello World",
                    feature=Feature.metadata(json.loads('{"a": 1, "b": "foo"}')),
                    labels={"label1": "val1", "label2": "val2"}
                ),
            ]
        ]
    

    def extract_query_embeddings(self, query: str) -> List[float]:
        return [1, 2, 3]

    @classmethod
    def schemas(cls) -> ExtractorSchema:
        """
        Returns a list of options for indexing.
        """
        return ExtractorSchema(
            features={"embedding": EmbeddingSchema(distance_metric="cosine", dim=3)},
        )


class MockExtractorNoInputParams(Extractor):
    def __init__(self):
        super().__init__()

    def extract(self, content: List[Content], params=None) -> List[List[Content]]:
        return [
            [
                Content.from_text(
                    text="Hello World", feature=Feature.embedding(value=[1, 2, 3])
                ),
                Content.from_text(
                    text="Pipe Baz", feature=Feature.embedding(value=[1, 2, 3])
                ),
            ]
        ]

    @classmethod
    def schemas(cls) -> ExtractorSchema:
        """
        Returns a list of options for indexing.
        """
        return ExtractorSchema(
            features={"embedding": EmbeddingSchema(distance_metric="cosine", dim=3)},
        )
