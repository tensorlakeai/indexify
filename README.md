<a name="readme-top"></a>
# Indexify 

[![Discord](https://dcbadge.vercel.app/api/server/VXkY7zVmTD?style=flat&compact=true)](https://discord.gg/VXkY7zVmTD)

## Create and Deploy Durable, Data-Intensive Agentic Workflows

**Indexify simplifies building and serving durable, multi-stage data-intensive workflows and exposes them as HTTP APIs or Python Remote APIs.**

<br />

> [!NOTE]  
> Indexify is the Open-Source core compute engine that powers Tensorlake's Serverless Workflow Engine for processing unstructured data.

### 💡 Use Cases

**Indexify** is a versatile data processing framework for all kinds of use cases, including:

* [Extracting and Indexing PDF Documents](examples/pdf_document_extraction/)
* [Scraping and Summarizing Websites](examples/website_audio_summary/)
* [Transcribing and Summarizing Audio Files](examples/video_summarization/)
* [Object Detection and Description](examples/object_detection/)
* [Knowledge Graph RAG and Question Answering](examples/knowledge_graph/)

### ⭐ Key Features

* **Multi-Cloud/Datacenter/Region:** Leverage Compute from other clouds with very little hassle and configuration.

* **Dynamic Routing:** Route data to different specialized compute functions distributed on a cluster based on conditional branching logic.
* **Local Inference:** Execute LLMs directly within workflow functions using LLamaCPP, vLLM, or Hugging Face Transformers in Python functions.
* **Distributed Processing:** Run functions in parallel across machines for scaleouts use-cases.
* **Resource Allocation:** Span workflows across GPU and CPU instances so that functions can be assigned to their optimal hardware.

## ⚙️ Installation

Install the Tensorlake SDK for building workflows and the Indexify CLI.

```bash
pip install indexify tensorlake
```

## 📚 A Minimal Example

Functions decorated with `@tensorlake_function()` are units of compute in your Workflow APIs. These functions can have data dependencies on other functions. <br></br>
Tensorlake functions are durable, i.e, if the function crashes or the node running the function is lost, it is going to be automatically retried on another running instance with the same input.
 <br></br>
You can run as many function instances on the cluster, inputs are going to be automatically load balanced across them when the workflow is called in parallel by other applications.

The example below is a workflow API that accepts some text, embeds the file, and writes it to a local vector-db. Each function could be placed on different classes of machines(CPU-only machines for chunking and writing to databases, NVIDIA GPUs for embedding)

```python
from pydantic import BaseModel
from tensorlake import tensorlake_function, Graph, Image, TensorlakeCompute
from typing import List, Union


class Text(BaseModel):
    text: str


class TextChunk(BaseModel):
    chunk: str
    page_number: int


class ChunkEmbedding(BaseModel):
    text: str
    embedding: List[float]


embedding_image = (
    Image()
    .name("text_embedding_image")
    .run("pip install langchain")
    .run("pip install sentence_transformer")
    .run("pip install langchain-text-splitters")
    .run("pip install chromadb")
    .run("pip install uuid")
)


# Chunk the text for embedding and retrieval
@tensorlake_function(input_encoder="json", image=embedding_image)
def chunk_text(input: dict) -> List[TextChunk]:
    text = Text.model_validate(input)
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=20,
        length_function=len,
        is_separator_regex=False,
    )
    texts = text_splitter.create_documents([text.text])
    return [
        TextChunk(chunk=chunk.page_content, page_number=i)
        for i, chunk in enumerate(texts)
    ]


# Embed a single chunk.
# Note: (Automatic Map) Indexify automatically parallelize functions when they consume an element
# from functions that produces a List
class Embedder(TensorlakeCompute):
    name = "embedder"
    image = embedding_image

    def __init__(self):
        from sentence_transformers import SentenceTransformer

        self._model = SentenceTransformer("all-MiniLM-L6-v2")

    def run(self, chunk: TextChunk) -> ChunkEmbedding:
        embeddings = self._model.encode(chunk.chunk)
        return ChunkEmbedding(text=chunk.chunk, embedding=embeddings)


class EmbeddingWriter(TensorlakeCompute):
    name = "embedding_writer"
    image = embedding_image

    def __init__(self):
        import chromadb

        self._chroma = chromadb.PersistentClient("./chromadb_tensorlake")
        self._collection = collection = self._chroma.create_collection(
            name="my_collection", get_or_create=True
        )

    def run(self, embedding: ChunkEmbedding) -> None:
        import uuid

        self._collection.upsert(
            ids=[str(uuid.uuid4())],
            embeddings=[embedding.embedding],
            documents=[embedding.text],
        )


# Constructs a compute graph connecting the three functions defined above into a workflow that generates
# runs them as a pipeline
graph = Graph(
    name="text_embedder",
    start_node=chunk_text,
    description="Splits, embeds and indexes text",
)
graph.add_edge(chunk_text, Embedder)
graph.add_edge(Embedder, EmbeddingWriter)
```

### Testing Locally 
You can test the workflow locally with only the `tensorlake` package installed. 

```python
invocation_id = graph.run(input={"text": "This is a test text"})
print(f"Invocation ID: {invocation_id}")

# You can get output from each function of the graph
embedding = graph.output(invocation_id, "embedder")
print(embedding)
```

## Deploying Workflows as APIs

Big Picture, you will deploy Indexify Server on a machine and run containers for each of the function in a workflow separately.

But first, we will show how to do this locally on a single machine.

## Start the Server 

Download a server release [from here](https://github.com/tensorlakeai/indexify/releases). Open a terminal and start the server.

```bash
./indexify-server -dev
```

## Start the Executors 

Executor is the component which is responsible for running your functions. On a terminal, where all the dependencies are installed, start an executor in `development` mode.

```bash
indexify-cli executor --dev
```

Change the code in the workflow to the following -
```python
from tensorlake import RemoteGraph
RemoteGraph.deploy(graph)
```

At this point, you now have a Graph endpoint on Indexify Server ready to be called as an API from any application.

## Invoke the Graph 
You can invoke the Graph as a REST API if the first function is configured to accept JSON payload. 

```curl
curl http://localhost:8900/namespaces/default/compute_graphs/text_embedder -d '{"text": "hello world"}'
```

You can invoke the Graph from Python too
```python
from tensorlake import RemoteGraph
remote_graph = RemoteGraph.by_name("text_embedder")
```

## Deploying to Production Clusters 

Deploying a workflow to production is a two step process -
1. Run the server 
2. Build and deploy containers to run the functions.

### Building and Deploying Containers

* First build and deploy container images that contains all the python and system dependencies of your code. They can be built using standard Docker build systems. For this example, we have a single image that can run all the functions. You can separate them to reduce the size of your images for more complex projects.

* Next Deploy the Containers 

```bash
docker run -d <your container image> indexify-cli executor --function default:text_embedder:chunk_document
docker run -d <your container image> indexify-cli executor --function default:text_embedder:embed_chunk
docker run -d <your container image> indexify-cli executor --function default:text_embedder:write_to_db
```

> Containers are treated as ephemeral, only one type of function is ever scheduled on a single container. We are starting two containers for placing one function in each of them.

## Scale To Zero
Indexify won't complain if you shut down the containers at night. It will still accept new API calls from external systems even it can't find machines to run functions. It will simply queue them up, and wait for functions to come up. It emits telemetry of pending tasks, waiting to be placed on functions which can be used as inputs to Autoscalers.

This is it! 

## Production Ready Distributed and Always-On Data Workflows 
* You have built a workflow API which is durable, capable of being distributed on many kinds of hardware and can handle limitless scale.
* You can use any Python libraries under the sun, any system packages and can use your favorite tools to package them into a container image.
* Deploying code is as simple as uploading code into the server, they get distributed and updated automatically. 


### 🗺️ Roadmap

#### ⏳ Scheduler

* **Function Batching:** Process multiple functions in a single batch to improve efficiency.
* **Data Localized Execution:** Boost performance by prioritizing execution on machines where intermediate outputs exist already.
* **Reducer Optimizations:** Optimize performance by batching the serial execution of reduced function calls.
* **Parallel Scheduling:** Reduce latency by enabling parallel execution across multiple machines.
* **Cyclic Graph Support:** Enable more flexible agentic behaviors by leveraging cycles in graphs.
* **Ephemeral Graphs:** Perform multi-stage inference and retrieval without persisting intermediate outputs.
* **Data Loader Functions:** Stream values into graphs over time using the `yield` keyword.

#### 🛠️ SDK

* **TypeScript SDK:** Build an SDK for writing workflows in Typescript.
