# Custom Extractors

Extractors being the components which extracts information from data are central to what is possible to do with data in Indexify. So we have made it possible to create custom extractors by developers and extend the functionality of Indexify.

## Creating a Custom Extractor
A custom extractor is a python class which inherits from the `Extractor` class in `indexify-extractors`. 

### Implement the info method 
The info method is called by Indexify to describe the extractor to users and register it with the system. The following parameters are required to be set in the info method:

- `name`: The name of the extractor
- `description`: A short description of the extractor
- `output_datatype`: It can either be `embeddings` or `attributes`. `embeddings` instruct Indexify to store the extracted data in a vector database. `attributes` data should be encoded in JSON and are stored in a JSON document store.
- `input_params`: It is an json schema object which describes the input parameters of the extractor. This allows users to pass in custom parameters to the extractor when they are binding the extractor to content. The schema is validated by Indexify before passing it to the extractor. 
- `output_schema`: It describes the schema of the output data. Since the output of an extractor can be encoded in JSON, the schema tells the application what to expect when the index created from running the extractor is read.

### Implement the extract method
The class must implement the `extract` method which takes in a  list of `Content` objects and returns a list of either `ExtractedEmbedding` or `ExtractedAttributes` objects. 

In the future we will add more data types, when we add extractors to generate data types such as audio, images and videos from ingested content. If you need to extend Indexify with any other output data types, please send a PR! 

#### Structure of Content
The `Content` type has two attributes `id` and `data`. 

- `id`:  is a string which uniquely identifies the content.
- `data`:  attribute holds the content payload. If it's text, it would be encoded in plain text, if it's images or some other binary content it would be raw bytes. 

We expect the users to bind content to extractors that can handle the content type.

### Enable the extractor in the executor configuration
In the config file of the executor, enable the extractor under the `extractors` section like this -

```yaml
extractors:
  - name: "my-custom-extractor"
    path : "mymodule.ClassName"
    driver: "python"
```


## Example
Let's build a new extractor that predicts sentiments of text. We will use Huggingface sentiment analysis pipeline to do this.

```python
import json
from .extractor_base import ExtractorInfo, Content, ExtractedAttributes, Extractor
from transformers import pipeline
from typing import List

class SentimentExtractor(Extractor):
   
    def __init__(self):
        self._pipeline = pipeline("sentiment-analysis")

    def extract(self, content: List[Content], params: dict[str, str]) -> List[ExtractedAttributes]:
        texts = [c.data for c in content]
        results = self._pipeline(texts)
        output = []
        for (content, result) in zip(content, results):
            data = json.dumps({"sentiment": result["label"], "score": result["score"]})
            output.append(ExtractedAttributes(content_id=content.id, json=data))
        return output

    def info(self) -> ExtractorInfo:
        schema = {"sentiment": "string",  "score": "float"}
        schema_json = json.dumps(schema)
        return ExtractorInfo(
            name="SentimentExtractor",
            description="An extractor that extracts sentiment from text.",
            output_datatype="attributes",
            input_params=json.dumps({}),
            output_schema= schema_json,
        )
```

Once the extractor is implemented, we need to enable it in the executor configuration. We can do this by adding the following to the executor config file.

```yaml
extractors:
  - name: "my-sentiment-extractor"
    path : "mymodule.SentimentExtractor"
    driver: "python"
```

After that the executor will automatically discover this extractor module, register with Indexify server and be able to run content through it when a user binds it to a data repository.