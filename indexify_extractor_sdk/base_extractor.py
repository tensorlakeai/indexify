from abc import ABC, abstractmethod
from typing import List, Type, Optional, Union
import json
from importlib import import_module
from typing import get_type_hints

from pydantic import BaseModel, Json, Field

class EmbeddingSchema(BaseModel):
    distance_metric: str
    dim: int

class ExtractorSchema(BaseModel):
    features: dict[str, Union[EmbeddingSchema, Json]] = Field(default_factory=dict)

class InternalExtractorSchema(BaseModel):
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
    @abstractmethod
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

    def schemas(self) -> InternalExtractorSchema:
        schema: ExtractorSchema = self._cls.schemas()
        embedding_schemas = {}
        for k,v in schema.features.items():
            if isinstance(v, EmbeddingSchema):
                embedding_schemas[k] = v
                continue
        json_schema = self._param_cls.model_json_schema() if self._param_cls else {}
        json_schema['additionalProperties'] = False
        return InternalExtractorSchema(embedding_schemas=embedding_schemas, input_params=json.dumps(json_schema))
    
def extractor_schema(module_name: str, class_name: str) -> InternalExtractorSchema:
    module = import_module(module_name)
    cls = getattr(module, class_name)
    param_cls = get_type_hints(cls.extract).get("params", None)
    schema: ExtractorSchema = cls.schemas()
    embedding_schemas = {}
    if schema is not None:
        for k,v in schema.features.items():
            if isinstance(v, EmbeddingSchema):
                embedding_schemas[k] = v
                continue
    json_schema = param_cls.model_json_schema() if param_cls else {}
    json_schema['additionalProperties'] = False
    return InternalExtractorSchema(embedding_schemas=embedding_schemas, input_params=json.dumps(json_schema))