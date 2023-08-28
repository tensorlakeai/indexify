from dataclasses import dataclass
from typing import List, Literal
from span_marker import SpanMarkerModel
from decimal import Decimal 
from enum import Enum
import json
from typing import Optional
from .base_extractor import Extractor, ExtractorInfo, Content, ExtractedAttributes
from pydantic import BaseModel

class EntityExtractionInputParams(BaseModel):
    overlap: int = 0
    text_splitter: Literal['char', 'token', 'recursive', 'new_line']  = 'new_line'

class EntityType(Enum): 
    def get_entity_type(type: str) -> str:
        entity_type_map = {
            "PER": "Person",
            "ORG": "Organization",
            "LOC": "Location",
            "ANIM": "Animal",
            "BIO": "Biological",
            "CELL": "Celestial",
            "DIS": "Disease",
            "EVE": "Event",
            "FOOD": "Food",
            "INST": "Instrument",
            "MEDIA": "Media",
            "PLANT": "Plant",
            "MYTH": "Mythological",
            "TIME": "Time",
            "VEHI": "Vehicle",
         }   
        return entity_type_map.get(type, "INVALID")
    

class EntityExtractor:
   
    def __init__(self):
        self._model = SpanMarkerModel.from_pretrained("tomaarsen/span-marker-xlm-roberta-base-multinerd")

    def extract(self, content: List[Content], params: dict[str, str]) -> List[ExtractedAttributes]:
        content_texts = [c.data for c in content]
        results = self._model.predict(content_texts)
        attributes = []
        for (i, ner_list) in enumerate(results):
            content_id = content[i].id
            for ner in ner_list:
                name = EntityType.get_entity_type(ner["label"])
                value = ner["span"]
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
            output_datatype="attributes",
            input_params=json.dumps(EntityExtractionInputParams.schema_json()),
            output_schema= schema_json,
        )

    