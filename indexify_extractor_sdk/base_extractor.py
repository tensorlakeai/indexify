from abc import ABC, abstractmethod
from typing import List, Type, Optional
import json
from importlib import import_module
from typing import get_type_hints

from pydantic import BaseModel, Json

class EmbeddingSchema(BaseModel):
    distance_metric: str
    dim: int

class ExtractorSchema(BaseModel):
    embedding_schemas: dict[str, EmbeddingSchema]
    input_params: Optional[str]

class Feature(BaseModel):
    feature_type: str
    name: str
    value: str

    @classmethod
    def embedding(cls, value: List[float], name: str="embedding"):
        return cls(feature_type="embedding", name=name, value=json.dumps(value))
    
    @classmethod
    def ner(cls, entity: str, value: str, score: float, name: str="ner"):
        return cls(feature_type="ner", name=name, value=json.dumps({"entity": entity, "value": value, "score": score}))
    
    @classmethod
    def metadata(cls, value: Json, name: str="metadata"):
        return cls(feature_type="metadata", name=name, value=json.dumps(value))

class Content(BaseModel):
    content_type: Optional[str]
    data: bytes 
    feature:Optional[Feature] = None

    @classmethod
    def from_text(cls, text: str, feature: Feature=None):
        return cls(content_type="text/plain", data=bytes(text, "utf-8"), feature=feature)
    
class Extractor(ABC):

    @abstractmethod
    def extract(
        self, content: List[Content], params: Type[BaseModel]=None) -> List[List[Content]]:
        """
        Extracts information from the content.
        """
        pass


    @classmethod
    def schemas(cls) -> ExtractorSchema:
        """
        Returns a list of options for indexing.
        """
        return NotImplemented

class ExtractorWrapper:

    def __init__(self, module_name: str, class_name: str):
        self._module = import_module(module_name)
        self._cls = getattr(self._module, class_name)
        self._param_cls = get_type_hints(self._cls.extract).get("params", None)
        self._instance = self._cls()

    def extract(self, content: List[Content], params: Json) -> List[List[Content]]:
        params_dict = json.loads(params)
        param_instance = self._param_cls.model_validate(params_dict) if self._param_cls else None
        content_list = []
        for c in content:
            content_list.append(Content(content_type=c.content_type, data=bytes(c.data)))
        return self._instance.extract(content_list, param_instance)

    def schemas(self) -> ExtractorSchema:
        return self._instance.schemas()