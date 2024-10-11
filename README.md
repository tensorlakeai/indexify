# Indexify
![Tests](https://github.com/tensorlakeai/indexify/actions/workflows/test.yaml/badge.svg?branch=main)
[![Discord](https://dcbadge.vercel.app/api/server/VXkY7zVmTD?style=flat&compact=true)](https://discord.gg/VXkY7zVmTD)

## Create and Deploy durable, Data-Intensive Agentic Workflows

Indexify simplifies building and serving durable, multi-stage workflows as inter-connected python functions and automagically deploys them as APIs.

Workflows end-points can be -
* `Pipelines` - linear sequence of functions.
* `Graphs` - parallel branches of pipelines, including conditional branching of data-flow.

Some of the use-cases that you can use Indexify for -

* [Scraping and Summarizing websites](examples/website_audio_summary/)
* [PDF Documents Extraction and Indexing](examples/pdf_document_extraction/)
* [Transcribing audio and summarization](examples/video_summarization/)
* [Knowledge Graph RAG and Question Answering](examples/knowledge_graph/)

### Key Features
* **Conditional Branching and Data Flow:** Router functions can conditionally choose one or more edges in Graph making it easy to invoke expert models based on inputs.
* **Local Inference:** Run LLMs in workflow functions using LLamaCPP, vLLM, or Hugging Face Transformers.
* **Distributed Map and Reduce:** Automatically parallelizes functions over sequences across multiple machines. Reducer functions are durable and invoked as map functions finish.
* **Version Graphs and Backfill:** Backfill API to update previously processed data when functions or models are updated.
* **Placement Constraints:** Allows graphs to span GPU instances and cost-effective CPU machines, with functions assigned to specific instance types.
* **Request Queuing and Batching:** Automatically queues and batches parallel workflow invocations to maximize GPU utilization.

## Installation
```bash
pip install indexify
```

## Basic Usage

Workflows are written as Python functions and are inter-connected as Graphs or Pipelines. Each function is a logical compute unit that can be retried upon failure or assigned to specific hardware.

#### 1: Define a Compute Pipeline or Graph 
```python
from pydantic import BaseModel
from indexify import indexify_function, indexify_router, Graph
from typing import List, Union

class Total(BaseModel):
    val: int = 0

@indexify_function()
def generate_numbers(a: int) -> List[int]:
    return [i for i in range(a)]

@indexify_function()
def square(i: int) -> int:
    return i ** 2

@indexify_function(accumulate=Total)
def add(total: Total, new: int) -> Total:
    total.val += new
    return total

g = Graph(name="sequence_summer", start_node=generate_numbers, description="Simple Sequence Summer")
g.add_edge(generate_numbers, square)
g.add_edge(square, add)
```

You can separate heavy tasks like local inference of LLMs from database write operations to prevent reprocessing data if a write fails. Indexify caches each function's output, so when you retry downstream processes, previous steps aren't repeated.

#### 2: Test the Graph In-Process
```python
invocation_id = g.run(a=10)
result = g.output(invocation_id, "add")
print(result)
```

Running Graph's in-process makes writing and testing Graphs easy, for production environments you would want an API to call them whenever there is data to process.

#### 3: Deploy your Graph as an API

Indexify server generates API endpoints for Compute Graphs, allowing external systems to invoke your workflows. The server can host multiple workflows and can execute functions across Graphs in parallel.

```bash
indexify-cli server-dev-mode
```

This starts the following processes on your terminal -

**Server**: Manages state of graphs, orchestrates functions, and stores function outputs.<br> 
API URL - http://localhost:8900 <br>
**Executor**: Runs python functions and coordinates execution state of functions with server.

Change the code above to deploy the graph as an API -

```python
from indexify import RemoteGraph

graph = RemoteGraph.deploy(g)
# for graphs which are already deployed
# graph = RemoteGraph.by_name("sequence_summer") 
invocation_id = graph.run(block_until_done=True, a=10)
result = graph.output(invocation_id, "add")
print(result)
```

This serializes your Graph code and uploads it to the server, and instantiates a new endpoint.
Everything else, remains the same in your application code that invokes the Graph to process data and retrieve outputs!

## More Topics 

* [Packaging Dependencies of Functions](https://docs.getindexify.ai/packaging-dependencies)
* [Programming Model](https://docs.getindexify.ai/key-concepts#programming-model)
* [Deploying Graph Endpoints using Docker Compose](https://docs.getindexify.ai/operations/deployment#docker-compose)
* [Deploying Graph Endpoints using Kubernetes](https://docs.getindexify.ai/operations/deployment#kubernetes)
* [Architecture of Indexify](https://docs.getindexify.ai/architecture)

### Roadmap

##### Scheduler

* Enable batching in functions
* Data Local function executions - Prioritize scheduling on machines where intermediate output lives for faster execution.
* Reducer optimizations - Being able to batch serial execution reduce function calls.
* Machine parallel scheduling for even lower latency.
* Support Cycles in the graphs for more flexible agentic behaviors in Graphs.
* Ephemeral Graphs - multi-stage inference and retrieval with no persistence of intermediate outputs
* Data Loader Functions - Produces a stream of values over time into Graphs, using the yield keyword.


##### SDK

* Build a Typescript SDK for writing workflows in Typescript
