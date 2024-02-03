from dataclasses import dataclass, asdict


@dataclass
class ExtractorBinding:
    extractor: str
    name: str
    content_source: str
    filters_eq: dict
    input_params: dict

    def __repr__(self) -> str:
        return f"ExtractorBinding(name={self.name} extractor={self.extractor})"

    def __str__(self) -> str:
        return self.__repr__()

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, json: dict):
        return ExtractorBinding(**json)
