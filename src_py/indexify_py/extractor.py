from abc import ABC, abstractmethod

from typing import Any

class Extractor(ABC):

    @abstractmethod
    def extract(self, content: Any, attrs: dict[str, Any]) -> Any:
        """
        Extracts information from the content.
        """
        pass


    @abstractmethod
    def get_name(self) -> str:
        """
        Returns the name of the extractor.
        """
        pass

    @abstractmethod
    def get_schema(self) -> str:
        """
        Returns the schema of the extractor in JSON schema format.
        It should have the following fields:
        {
            "type": "extraction object",
            "description": "string",
            "properties": {
                "property_name_1": "property_type",
                "property_name_2": "property_type",
            }
        }
        """
        pass