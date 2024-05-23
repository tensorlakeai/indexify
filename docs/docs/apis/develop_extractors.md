# Developing a new Extractor

When you are working on a use case for which we might not have an extractor already you can write a new extractor, and deploy in your cluster and add new capabilities to Indexify.

## Concepts

An extractor receives unstructured data in a `Content` object and transforms the content into one or many `Content`, each output content can optionally have a `Feature` added to it during extraction. For example, you could split a PDF content into three content with their text and corresponding embedding or some other metadata such as tabular information encoded as JSON metadata.

The content object has the following properties -

* **data** - The unstructured data encoded as raw bytes.
* **content_type** - The mime type of the data. For example, `text/plain`, `image/png`, etc. This allows you to decode the bytes correctly.
* **labels** - Optional Key Value metadata associated with the content provided by users or added by Indexify. Labels are meant for filtering content while deciding which bindings are invoked on them or for storing user defined opaque metadata 
* **Feature** - Optional Feature associated with the content, such as embedding or JSON metadata. Embeddings are stored in indexes in Vector Store and JSON metadata are stored in structured store such as Postgres. Features are searchable, if they are embedding you can perform KNN search on the resulting index, if it's JSON you could do JSON path queries on them.

The **Content** object is [defined here](https://github.com/tensorlakeai/indexify/blob/11346c29055f16d397fc0901ec10139cdc945134/indexify_extractor_sdk/base_extractor.py#L48) 

### Feature
Feature is some form of extracted information from unstructured data. Embedding, or JSON metadata are the possible features for now. Features extracted are indexed and searchable.
Features can be easily constructed from [helper methods](https://github.com/tensorlakeai/indexify/blob/11346c29055f16d397fc0901ec10139cdc945134/indexify_extractor_sdk/base_extractor.py#L37)
You can optionally give features a name such as `my_custom_text_embedding`, we use the names as sufixes of index names.

## Install the Extractor SDK 
```shell
pip install indexify-extractor-sdk
```

## Start from a template

The following command will create a template for a new extractor in the current directory. 

```shell
git clone https://github.com/tensorlakeai/indexify-extractor-template
```

## Implement the Extractor 
The template contains a `MyExtractor` extractor in the `custom_extractor.py` file. Implement the extract method, which accepts a `Content` and produces a list of `Content`. The output content list is usually some form of transformation of the input and some features related to it. The valid output features are `Embedding` and `Metadata`. 

```python
def extract(self, content: Content) -> List[Content]:
    """
    Extracts features from content.
    """
    output: List[Content] = []
    chunks = content.chunk()
    for chunk in chunks:
        embedding = get_embedding(chunk)
        entities = run_ner_model(chunk)
        embed_chunk = Content.from_text(text=chunk, feature=Feature.embedding(name="text_embedding", values=embedding))
        metadata_chunk = Content.from_text(text=chunk, feature=Feature.metadata(name="metadata", json.dumps(entities))),
        output.append([embed_chunk, metadata_chunk])
    return output
```

In this example we iterate over a list of content, chunk each content, run a NER model and an embedding model over each chunk and return them as features along with the chunks of text.

!!! note "Extractor Dependencies"

    Use any python or native system dependencies in your extractors because we can package them in a container to deploy them to production.


## Extractor Metadata
Add a name to your extractor, a description of what it does and python and system dependencies. These goes in attributes/properties of your Extractor class -

* **name** - The name of the extractor. We use the name of the extractor also to name the container package.
* **description** - Long description of the extractor
* **python_dependencies** -  List of python dependencies that you are importing in the extractor. Example - `["torch", "transformers"]`
* **system_dependencies** - List of system dependencies of the extractor such as any native dependencies of the model or packages you are using. Example - `["curl", "protobuf-compiler"]`
* **input_mime_types** - The list of input data types the extractor can handle. We use standard mime types as the API. Default is `["text/plain]`, and you can override or specify which ones your extractor supports from the [list here](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types)

#### Test the extractor locally

Extractors are just python modules so you can write a unit test like any any other python module. You should also test the extractor using the indexify binary to make sure it works as expected. 

```shell
indexify-extractor run-local custom_extractor:MyExtractor --text "hello world"
```

#### Join with Control Plane
You can join the extractor with the Indexfy server for it to receive streams of content to extract from
```shell
indexify-extractor join-server
```

#### Package the extractor
Once you have tested the package, package it into a container. From here the extractor is deployable to production using Kubernetes, ECS or other container based deployment platforms.

```shell
indexify-extractor package </path/to/extractor>:<ExtractorClass>
```

If you want to package an extractor in a container that support NVIDIA GPU, you can pass the `--gpu` flag to the package command.

The packaged extractors should also be visible as a docker container locally.
```shell
docker images
```