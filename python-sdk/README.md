# Indexify Python SDK

[![PyPI version](https://badge.fury.io/py/indexify.svg)](https://badge.fury.io/py/indexify)
[![Discord](https://dcbadge.vercel.app/api/server/VXkY7zVmTD?style=flat&compact=true)](https://discord.gg/VXkY7zVmTD)

This is the Python SDK to build real-time continuously running unstructured data processing pipelines with Indexify.

Start by writing and testing your pipelines locally using your data, then deploy them into the Indexify service to process data in real-time at scale.

## Installation

```shell
pip install indexify
```

## Examples 
**[PDF Document Extraction](../examples/pdf_document_extraction/workflow.py)**
1. Extracts text, tables and images from an ingested PDF file
2. Indexes the text using MiniLM-L6-v2, the images with CLIP
3. Writes the results into a vector database.

**[Youtube Transcription Summarizer](../examples/video_summarization/workflow.py)**
1. Downloads Youtube Video
2. Extracts audio from the video and transcribes using `Faster Whisper` 
3. Uses Llama 3.1 backed by `Llama.cpp` to understand and classify the nature of the video.
4. Routes the transcription dynamically to one of the transcription summarizer to retain specific summarization attributes.
5. Finally the entire transcription is embedded and stored in a vector database for retrieval.

## Quick Start
1. Write data processing functions in Python and use Pydantic objects for returning complex data types from functions
2. Connect functions using a graph interface. Indexify automatically stores function outputs and passes them along to downstream functions. 
3. If a function returns a list, the downstream functions will be called with each item in the list in **parallel**.
4. The input of the first function becomes the input to the HTTP endpoint of the Graph.

## Functional Features
1. There is **NO** limit to volume of data being ingested since we use blob stores for storing metadata and objects
2. The server can handle 10s of 1000s of files being ingested into the graphs in parallel. 
3. The scheduler reacts under 8 microseconds to ingestion events, so it's suitable for workflows which needs to run in realtime.
4. Batch ingestion is handled gracefully by batching ingested data and scheduling for high throughput in production settings.

```python
from pydantic import BaseModel
from indexify import indexify_function
from typing import Dict, Any, Optional, List

# Define function inputs and outputs
class Document(BaseModel):
    text: str
    metadata: Dict[str, Any]

class TextChunk(BaseModel):
    text: str
    metadata: Dict[str, Any]
    embedding: Optional[List[float]] = None


# Decorate a function which is going to be part of your data processing graph
@indexify_function()
def split_text(doc: Document) -> List[TextChunk]:
    midpoint = len(doc.text) // 2
    first_half = TextChunk(text=doc.text[:midpoint], metadata=doc.metadata)
    second_half = TextChunk(text=doc.text[midpoint:], metadata=doc.metadata)
    return [first_half, second_half]

# Any requirements specified is automatically installed in production clusters
@indexify_function(requirements=["langchain_text_splitter"])
def compute_embedding(chunk: TextChunk) -> TextChunk:
    chunk.embedding = [0.1, 0.2, 0.3]
    return chunk

# You can constrain functions to run on specific executors 
@indexify_function(executor_runtime_name="postgres-driver-image")
def write_to_db(chunk: TextChunk):
    # Write to your favorite vector database
    ...

## Create a graph
from indexify import Graph

g = Graph(name="my_graph", start_node=split_text)
g.add_edge(split_text, compute_embedding)
g.add_edge(embed_text, write_to_db)
```

## Graph Execution
Every time the Graph is invoked, Indexify will provide an `Invocation Id` which can be used to know about the status of the processing and any outputs from the Graph.

## Run the Graph Locally
```python
from indexify import IndexifyClient

client = IndexifyClient(local=True)
client.register_graph(g)
invocation_id = client.invoke_graph_with_object(g.name, Document(text="Hello, world!", metadata={"source": "test"}))
graph_outputs = client.graph_outputs(g.name, invocation_id)
```

## Deploy the Graph to Indexify Server for Production
> Work In Progress - The version of server that works with python based graphs haven't been released yet. It will be shortly released. Join discord for development updates. 
```python
from indexify import IndexifyClient

client = IndexifyClient(service_url="http://localhost:8900")
client.register_graph(g)
```

#### Ingestion into the Service
Extraction Graphs continuously run on the Indexify Service like any other web service. Indexify Server runs the extraction graphs in parallel and in real-time when new data is ingested into the service.

```python
output_id = client.invoke_graph_with_object(g.name, Document(text="Hello, world!", metadata={"source": "test"}))
```

#### Retrieve Graph Outputs for a given ingestion object
```python
graph_outputs = client.graph_outputs(g.name, output_id)
```

#### Retrieve All Graph Inputs 
```python
graph_inputs = client.graph_inputs(g.name)
```
