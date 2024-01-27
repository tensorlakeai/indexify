# Developing a new Extractor

When you are working on a use case for which we might not have an extractor already you can write your own extractor, and deploy in your cluster, and also publish to our extractor hub for others to use! 

## Start from a template

The following command will create a template for a new extractor in the current directory. 

```shell
indexify extractor new --path my-extractor
```

## Implement the Extractor 
The template creates a `MyExtractor` class in the `custom_extractor.py` file. Implement the extract method, which accepts a `Content` and prouduces a list of `Content`. The output content is usually some form of transformation of the input to the extractors and some features related to it. The valid output features are `Embedding` and `JSON`. Chunks of document goes in the `Content` in the data payload of the `Content`. 

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
Add a name to your extractor, a description of what it does and python and system dependencies. These goes in attributes/properties of your Extractor class.
** name ** - The name of the extractor. We use the name of the extractor also to name the container package.

** description ** - Long description of the extractor

** python_dependencies ** -  List of python dependencies that you are importing in the extractor. Example - `["torch", "transformers"]`

** system_dependencies ** - List of system dependencies of the extractor such as any native dependencies of the model or packages you are using. Example - `["curl", "protobuf-compiler"]`

** input_mime_types ** - The list of input data types the extractor can handle. We use standard mime types as the API. Default is `["text/plain]`, and you can override or specify which ones your extractor supports from the [list here](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types)

#### Test the extractor locally

Extractors are just python modules so you can write a unit test like any any other python module. You should also test the extractor using the indexify binary to make sure it works as expected. 

```shell
indexify extractor extract -e my-extractor/custom_extractor.py:MyExtractor --text "hello world"
```

#### Join with Control Plane
It's a good idea to test the behavior of the extractor with the Indexify Control Plane locally to make sure it works as expected. 

```shell
indexify extractor start -e my-extractor/indexify.yaml --control-plane-addr 172.21.0.2:8950
```

#### Update the extractor configuration
You can include the python and system dependencies of the extractor in the configuration file. Update the indexify.yaml file with the dependencies of the extractor. 

```yaml
name: my-extractor
version: 1
description: "Description of the Extractor goes here"
# Rename the file and the class name if you wish to
module: custom_extractor.py:MyExtractor
gpu: false
# Add all the python dependencies here which are required for the extractor to work
python_dependencies:
  - torch
# Add all the system dependencies here. We use a ubuntu base image, so the package names
# should be available in ubuntu
system_dependencies:
  - ffmpeg

```

#### Package the extractor
Once you have tested the package, pakcage it into a container. From here the extractor is deployable to any environment. You can share the extractor on our Hub for other developers to know about it! 

```shell
indexify extractor package -v -c my-extractor/indexify.yaml
```

The packaged extractors should also be visible as a docker container locally.
```shell
docker images
```

#### Test the packaged extractor

```shell
indexify extractor extractor --name your-name/indexify-extractor --text "hello world"
```

#### Deploy the extractor to production
Once the extractor is packaged, it can be deployed to any environment as long as the Indexify control plane can access it. Point the extractor to the production control plane and that's all! 
