from dataclasses import dataclass
from typing import List, Literal
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline
from decimal import Decimal 
from enum import Enum
import json
from typing import Optional
from .extractor_base import Extractor, ExtractorInfo, Content
from pydantic import BaseModel

class EntityExtractionInputParams(BaseModel):
    overlap: int = 0
    text_splitter: Literal['char', 'token', 'recursive', 'new_line']  = 'new_line'

@dataclass
class ExtractedAttributes:
    content_id: str
    json: str

class EntityType(Enum): 
    def get_entity_type(type: str) -> str:
        entity_type_map = {
            "B-PER": "Person",
            "B-ORG": "Organization",
            "I-ORG": "Organization",
            "B-LOC": "Location",
            "I-LOC": "Location"
         }   
        return entity_type_map.get(type, "INVALID")
    

class EntityExtractor:
   
    def __init__(self, model_name: str = "dslim/bert-base-NER"):
        self.model_name = model_name
        # Load model - https://huggingface.co/dslim/bert-base-NER
        self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self._model = AutoModelForTokenClassification.from_pretrained(self.model_name)
        self._ctx_pipeline = pipeline("ner", model=self._model, tokenizer=self._tokenizer)

    def extract(self, content: List[Content], params: dict[str, str]) -> List[ExtractedAttributes]:
        content_texts = [c.data for c in content]
        results = self._ctx_pipeline(content_texts)
        attributes = []
        for (i, ner_list) in enumerate(results):
            content_id = content[i].id
            for ner in ner_list:
                name = EntityType.get_entity_type(ner["entity"])
                value = ner["word"]
                score = ner["score"]
                data = json.dumps({"entity": name, "value": value, "score": str(score)})
                attributes.append(ExtractedAttributes(content_id=content_id, json=data))
        return attributes 

    def info(self) -> ExtractorInfo:
        schema = {"entity": "string", "value": "string", "score": "float"}
        schema_json = json.dumps(schema)
        return ExtractorInfo(
            name="EntityExtractor",
            description="EntityExtractor",
            input_params="{'type': 'object'}",
            output_datatype="attributes",
            input_params=EntityExtractionInputParams.model_json_schema(),
            output_schema= schema_json,
        )

    