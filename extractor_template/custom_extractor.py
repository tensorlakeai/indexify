from pydantic import BaseModel

from typing import List

from indexify_extractor_sdk import Extractor, Content, Feature, EmbeddingSchema, ExtractorSchema

import json

class InputParams(BaseModel):
    a: int = 0
    b: str = ""

class MyExtractor(Extractor):
    def __init__(self):
        super().__init__()

    def extract(
        self, content: List[Content], params: InputParams
    ) -> List[List[Content]]:
        return [
            [
                ## If the name of the embedding field in the schema is anything besides "embedding", 
                # you must specify the name of the field in the Feature.embedding call.
                # Feature.embedding(value=[1, 2, 3], name="my_embedding")
                Content.from_text(
                    text="Hello World", feature=Feature.embedding(value=[1, 2, 3])
                ),
                Content.from_text(
                    text="Pipe Baz", feature=Feature.embedding(value=[1, 2, 3])
                ),
                Content.from_text(
                    text="Hello World",
                    feature=Feature.ner(entity="Kevin Durant", value="PER", score=0.9),
                ),
            ]
        ]

    def schemas(self) -> ExtractorSchema:
        """
        Returns a list of options for indexing.
        """
        return ExtractorSchema(
            output_schemas={"embedding": EmbeddingSchema(distance_metric="cosine", dim=3)},
            input_params=json.dumps(InputParams.model_json_schema()),
        )