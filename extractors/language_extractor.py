from typing import List, Literal
from span_marker import SpanMarkerModel
from decimal import Decimal
from enum import Enum
import json
from indexify_extractor_sdk.base_extractor import ExtractorInfo, Content, Attributes, Extractor
from lingua import Language, LanguageDetectorBuilder
from pydantic import BaseModel

class LanguageExtractionInputParams(BaseModel):
    overlap: int = 0
    text_splitter: Literal["char", "token", "recursive", "new_line"] = "new_line"

class LanguageExtractionOutputSchema(BaseModel):
    entity: str
    value: str
    score: Decimal

class LanguageExtractor(Extractor):
    def __init__(self):
        self._model = LanguageDetectorBuilder.from_all_languages().with_low_accuracy_mode().build()

    def extract(
        self, content: List[Content], params: dict[str, str]
    ) -> List[Attributes]:
        content_texts = [c.data for c in content]
        content_texts = [c.data for c in content]
        attributes = []
        for i, x in enumerate(content):
            language = self._model.detect_language_of(content_texts[i])
            confidence = self._model.compute_language_confidence(content_texts[i], language)
            data = json.dumps({"entity": "language", "value": language.name, "score": str(confidence)})
            attributes.append(
                    Attributes(
                        content_id=x.id, text=content_texts[i], attributes=data
                    )
                )
        return attributes

    def info(self) -> ExtractorInfo:
        input_params = LanguageExtractionInputParams()
        return ExtractorInfo(
            name="LanguageExtractor",
            description="LanguageExtractor",
            input_params=input_params,
            output_schema=LanguageExtractionOutputSchema,
        )
