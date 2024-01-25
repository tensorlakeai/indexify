from .base_extractor import Extractor, Content, Feature

from typing import List

from pydantic import BaseModel

import json


class InputParams(BaseModel):
    a: int = 0
    b: str = ""


class MockExtractor(Extractor):

    input_mime_types = ["text/plain", "application/pdf", "image/jpeg"]

    def __init__(self):
        super().__init__()

    def extract(
        self, content: Content, params: InputParams
    ) -> List[Content]:
        return [
                Content.from_text(
                    text="Hello World",
                    feature=Feature.embedding(values=[1, 2, 3]),
                    labels={"url": "test.com"},
                ),
                Content.from_text(
                    text="Pipe Baz",
                    feature=Feature.embedding(values=[1, 2, 3]),
                    labels={"url": "test.com"},
                ),
                Content.from_text(
                    text="Hello World",
                    feature=Feature.metadata(
                        json.loads('{"a": 1, "b": "foo"}')),
                    labels={"url": "test.com"},
                ),
            ]

    def sample_input(self) -> Content:
        return Content.from_text("hello world")


class MockExtractorNoInputParams(Extractor):
    def __init__(self):
        super().__init__()

    def extract(self, content: Content, params=None) -> List[Content]:
        return [
                Content.from_text(
                    text="Hello World",
                    feature=Feature.embedding(values=[1, 2, 3])
                ),
                Content.from_text(
                    text="Pipe Baz",
                    feature=Feature.embedding(values=[1, 2, 3])
                ),
            ]

    def sample_input(self) -> Content:
        return Content.from_text("hello world")
