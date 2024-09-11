# Indexify Extractor SDK

[![PyPI version](https://badge.fury.io/py/indexify-extractor-sdk.svg)](https://badge.fury.io/py/indexify-extractor-sdk)

Indexify Extractor SDK is for developing new extractors to extract information
from any unstructured data sources.

We already have a few extractors here - https://github.com/tensorlakeai/indexify
If you don't find one that works for your use-case use this SDK to build one.

## Install the SDK

Install the SDK from PyPi

```bash
virtualenv ve
source ve/bin/activate
pip install indexify-extractor-sdk
```

## Implement the extractor SDK

There are two ways to implement an extractor. If you don't need any
setup/teardown or additional functionality, check out the decorator:

```python
from indexify_extractor_sdk import Content, extractor

@extractor()
def my_extractor(content: Content, params: dict) -> List[Content]:
    return [
        Content.from_text(
            text="Hello World",
            features=[
                Feature.embedding(values=[1, 2, 3]),
                Feature.metadata(json.loads('{"a": 1, "b": "foo"}')),
            ],
            labels={"url": "test.com"},
        ),
        Content.from_text(
            text="Pipe Baz",
            features=[Feature.embedding(values=[1, 2, 3])],
            labels={"url": "test.com"},
        ),
    ]
```

Note: `@extractor()` takes many parameters, check out the documentation for more
details.

For more advanced use cases, check out the class:

```python
from indexify_extractor_sdk import Content, Extractor, Feature
from pydantic import BaseModel

class InputParams(BaseModel):
    pass

class MyExtractor(Extractor):
    input_mime_types = ["text/plain", "application/pdf", "image/jpeg"]

    def __init__(self):
        super().__init__()

    def extract(self, content: Content, params: InputParams) -> List[Content]:
        return [
            Content.from_text(
                text="Hello World",
                features=[
                    Feature.embedding(values=[1, 2, 3]),
                    Feature.metadata(json.loads('{"a": 1, "b": "foo"}')),
                ],
                labels={"url": "test.com"},
            ),
            Content.from_text(
                text="Pipe Baz",
                features=[Feature.embedding(values=[1, 2, 3])],
                labels={"url": "test.com"},
            ),
        ]

    def sample_input(self) -> Content:
        return Content.from_text("hello world")

```

## Test the extractor

You can run the extractor locally using the command line tool attached to the
SDK like this, by passing some arbitrary text or a file.

```bash
indexify-extractor local my_extractor:MyExtractor --text "hello"
```

## Deploy the extractor

Once you are ready to deploy the new extractor and ready to build pipelines with
it. Package the extractor and deploy as many copies you want, and point it to
the indexify server. Indexify server has two addresses, one for sending your
extractor the extraction task, and another endpoint for your extractor to write
the extracted content.

```
indexify-extractor join-server --coordinator-addr localhost:8950 --ingestion-addr:8900
```
