# Custom Extractors

Extractors extracts structured data or emebedding from content ingested into Indexify. You can write new extractors and extend the functionality of Indexify.

## Creating a Custom Extractor
A custom extractor is a python class which inherits from the `Extractor` class in the `indexify-extractor-sdk` library. 

## A Custom PDF Extractor
We will implement an extractor to extract embedding of text, and a structured document to extract named entities, topics.

```python
class PdfExtractor(Extractor):
   
    def __init__(self):
        pass

    def extract(self, content: Content, params: dict[str, str]) -> List[Union[Feature, Content]]:
        pass

    def sample_input(self) -> Tuple[Content, Type[BaseModel]]:
        Content.from_text(text="Hello World")
```

**extract** - Takes a `Content` object which have the bytes of unstructured data and the mime-type. You can pass a list of JSON, text, video, audio and documents into the extract method. It should return a list of transformed or derived content, or a list of features. 
Examples - 
- Text Chunking: Input(Text) -> List(Text)
- Audio Transcription: Input(Audio) -> List(Text)
- Speaker Diarization: Input(Audio) -> List(JSON of text and corresponding speaker ids)
- PDF Extraction: Input(PDF) -> List(Text, Images and JSON representation of tables)
- PDF to Markdown: Input(PDF) -> List(Markdown)

**sample_input**: A sample input your extractor can process and a sample input config. This will be run when the extractor starts up to make sure the extractor is functioning properly.


### Describe dependencies 
Add a `requirements.txt` file to the folder if it has any python dependencies.


### Package the extractor
```bash 
indexify-extractor package custom_extractor:MyExtractor
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