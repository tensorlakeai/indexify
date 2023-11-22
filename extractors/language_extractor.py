import json
from pydantic import BaseModel
from typing import List, Literal

from indexify_extractor_sdk import (
    Extractor,
    Feature,
    ExtractorSchema,
    Content,
)
from lingua import LanguageDetectorBuilder


class LanguageExtractionInputParams(BaseModel):
    overlap: int = 0
    text_splitter: Literal["char", "token", "recursive", "new_line"] = "new_line"

class LanguageExtractor(Extractor):
    """
    Extractor class for detecting the language of given content.
    """

    def __init__(self):
        super().__init__()
        self._model = LanguageDetectorBuilder.from_all_languages().build()

    def extract(
        self, content: List[Content], params: LanguageExtractionInputParams
    ) -> List[List[Content]]:
        content_texts = [c.data.decode("utf-8") for c in content]
        out = []
        for i, x in enumerate(content):
            language = self._model.detect_language_of(content_texts[i])
            confidence = self._model.compute_language_confidence(content_texts[i], language)
            # TODO: Could be modified depending on the database we have
            data = {"language": language.name, "score": str(confidence)}
            out.append(
                [Content.from_text(
                    text=content_texts[i],
                    feature=Feature.metadata(value=data, name="language"),
                )]
            )
        return out

    def schemas(self) -> ExtractorSchema:
        """
        Returns a list of options for indexing.
        """
        input_params = LanguageExtractionInputParams()
        # TODO If it's metadata, how do we extract things
        # This extractor does not return any embedding, only a dictionary!
        return ExtractorSchema(
            embedding_schemas={},
            input_params=json.dumps(input_params.model_json_schema()),
        )

