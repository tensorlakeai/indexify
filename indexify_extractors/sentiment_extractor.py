import json
from .base_extractor import ExtractorInfo, Content, ExtractedAttributes, Extractor
from transformers import pipeline
from typing import List

class SentimentExtractor(Extractor):
   
    def __init__(self):
        self._pipeline = pipeline("sentiment-analysis")

    def extract(self, content: List[Content], params: dict[str, str]) -> List[ExtractedAttributes]:
        texts = [c.data for c in content]
        results = self._pipeline(texts)
        output = []
        for (content, result) in zip(content, results):
            content_id = content.id
            data = json.dumps({"sentiment": result["label"], "score": result["score"]})
            output.append(ExtractedAttributes(content_id=content_id, json=data))
        return output

    def info(self) -> ExtractorInfo:
        schema = {"sentiment": "string",  "score": "float"}
        schema_json = json.dumps(schema)
        return ExtractorInfo(
            name="SentimentExtractor",
            description="An extractor that extracts sentiment from text.",
            output_datatype="attributes",
            input_params=json.dumps({}),
            output_schema= schema_json,
        )


 