from dataclasses import dataclass
from typing import List
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline
from decimal import Decimal 
from enum import Enum
from typing import Optional


@dataclass
class EntityAttributes:
    score: Decimal 

@dataclass
class Entity:
   name: str
   value: str
   attributes: EntityAttributes


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

        
    def extract(self, text: str) -> List[Entity]:
         ner_list = self._ctx_pipeline(text)
         entities = []

         for ner in ner_list:
             name = EntityType.get_entity_type(ner["entity"])
             value = ner["word"]
             score = ner["score"]
             entity = Entity(name=name, value=value, attributes=EntityAttributes(score=score))
             entities.append(entity)
         return entities

    