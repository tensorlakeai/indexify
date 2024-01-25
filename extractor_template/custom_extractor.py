from pydantic import BaseModel

from typing import List

from indexify_extractor_sdk import Extractor, Content, Feature
from indexify_extractor_sdk.base_extractor import Content
import json

class InputParams(BaseModel):
    a: int = 0
    b: str = ""

class MyExtractor(Extractor):
    name = "your-docker-hub-username/MyExtractor"
    description = "Description of the extractor goes here."

    # Any python dependencies included in the extractor must be listed here.
    python_dependencies = ["torch", "transformers"]

    # Any system dependencies that the python code here depends on needs to be listed here
    # We use Ubuntu base images, so any ubuntu package can be installed here.
    system_dependencies = []

    # The mime types of content that this extractor can process. Any content ingested into
    # Indexify which does not match one of these mime types will not be sent to this extractor.
    input_mime_types = ["text/plain"]

    def __init__(self):
        super().__init__()

    def extract(self, content: Content, params: InputParams) -> List[Content]:
        return [
                ## If the name of the embedding field in the schema is anything besides "embedding",
                # you must specify the name of the field in the Feature.embedding call.
                # Feature.embedding(value=[1, 2, 3], name="my_embedding")
                Content.from_text(
                    text="Hello World", feature=Feature.embedding(values=[1, 2, 3])
                ),
                Content.from_text(
                    text="Pipe Baz", feature=Feature.embedding(values=[1, 2, 3])
                ),
                Content.from_text(
                    text="Hello World",
                    feature=Feature.metadata(value=json.dumps({"key": "value"})),
                ),
            ]

    # Provide some representative sample input that the extractor can process.
    def sample_input(self) -> Content:
        Content.from_text(text="Hello World")


if __name__ == "__main__":
    # You can run the extractor by simply invoking this python file
    # python custom_extractor.py and that would run the extractor
    # with the sample input provided.
    MyExtractor().extract_sample_input()
