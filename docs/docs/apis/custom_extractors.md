# Custom Extractors

Extractors extracts structured data or emebeddings from content ingested into Indexify. You can write new extractors and extend the functionality of Indexify.

## Creating a Custom Extractor
A custom extractor is a python class which inherits from the `Extractor` class in the `indexify-extractor-sdk` library. 

## A Custom PDF Extractor
We will implement an extractor to extract embedding of text, and a structured document to extract named entities, topics.

```python
class PdfExtractor(Extractor):
   
    def __init__(self):
        pass

    def extract(self, content: List[Content], params: dict[str, str]) -> List[ExtractedAttributes]:
        pass

    def info(self) -> ExtractorInfo:
        pass
```

### Write a config file to package the extractor
Create a config file that contains name, description and dependencies of the extractor in the same directory with the source code of the extractor.
```yaml
name: diptanu/pdf-extractor
version: 1
module: pdf_extractor.PDFExtractor
gpu: false
python_dependencies:
  - torch
  - transformers
  - pypdf[full]
#system_dependencies:
#  - Add any system dependencies here
```

### Package the extractor
```bash 
indexify package --verbose --config-path pdf_extractor_config.yaml
```
This should create an export a docker container with the extractor called `diptanu/pdf-extractor:1`. 

### Start using the extractor
Now you can point the executor to the coordinator address of the indexify service and start binding the extractor to a data bucket.

```bash
docker run -it --rm diptanu/pdf-extractor:1 executor  --coordinator-addr=172.21.0.2:8950
```

## Details about the Extractor Class

### The info method 
The info method should return the `ExtractorInfo` object which describes the extractor. 

- `input_params`: It is an json schema object which describes the input parameters of the extractor. This allows extractors to be parameterized at the time of binding with a data bucket. 
- `output_schema`: It describes the schema of the output data. 

### The extract method
The class must implement the `extract` method which takes in a  list of `Content` objects and returns a list of either `ExtractedEmbedding` or `ExtractedAttributes` objects. 

#### Structure of Content
The `Content` type has two attributes `id` and `data`. 

- `id`:  is a string which uniquely identifies the content.
- `data`:  attribute holds the content payload. If it's text, it would be encoded in plain text, if it's images or some other binary content it would be raw bytes. 