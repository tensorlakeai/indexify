# Indexify
![Tests](https://github.com/tensorlakeai/indexify/actions/workflows/test.yaml/badge.svg?branch=main)
[![Discord](https://dcbadge.vercel.app/api/server/VXkY7zVmTD?style=flat&compact=true)](https://discord.gg/VXkY7zVmTD)

## Create and Deploy durable, Data-Intensive Agentic Workflows

Indexify simplifies building and serve durable, multi-stage workflows as python functions inter-connected as graphs and automagically deploys them as APIs.

Some of the use-cases that you can use Indexify for -

* [Scraping and Summarizing websites](examples/website_audio_summary/)
* [PDF Documents Extraction and Indexing](examples/pdf_document_extraction/)
* [Transcribing audio and summarization](examples/website_audio_summary/)

### Key Features
* **Conditional Branching and Data Flow:** Router functions can conditionaly chose one or more edges in Graph making it easy to invoke expert models based on inputs.
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

Workflows are written as Python functions and are connected as Graphs. Each function is a logical compute unit that can be retried upon failure or assigned to specific hardware.

#### 1: Create a Compute Graph
```python
from pydantic import BaseModel
from indexify import indexify_function, indexify_router, Graph
from typing import List, Union

class Total(BaseModel):
    val: int = 0

@indexify_function()
def generate_numbers(a: int) -> List[int]:
    return [i for i in range(a)]

@indexify_function(accumulate=Total)
def add(total: Total, new: int) -> Total:
    total.val += new
    return total

@indexify_function()
def square(total: Total) -> int:
    return total.val ** 2

@indexify_function()
def cube(total: Total) -> int:
    return total.val ** 3

@indexify_router()
def dynamic_router(val: Total) -> List[Union[square, cube]]:
    if val.val % 2:
        return [square]
    return [cube]

if __name__ == '__main__':
    g = Graph(name="sequence_summer", start_node=generate_numbers, description="Simple Sequence Summer")
    g.add_edge(generate_numbers, add)
    g.add_edge(add, dynamic_router)
    g.route(dynamic_router, [square, cube])

    from indexify import create_client

    client = create_client(in_process=True)
    client.register_compute_graph(g)

    invocation_id = client.invoke_graph_with_object("sequence_summer", block_until_done=True, a=3)
    result = client.graph_outputs("sequence_summer", invocation_id, "squared")
    print(result)
    result = client.graph_outputs("sequence_summer", invocation_id, "tripled")
    print(result)
```

You can separate heavy tasks like local inference of LLMs from database write operations to prevent reprocessing data if a write fails. Indexify caches each function's output, so when you retry downstream processes, previous steps aren't repeated.

The Graphs are hosted in the Indexify Server and API calls to these graphs are automatically queued and routed based on the graph’s topology, eliminating the need for RPC libraries, Kafka, or additional databases to manage internal state and communication across different processes or machines.

#### 2: Register and Invoke the Compute Graph
```python
from indexify import create_client

g = Graph(name="sequence_summer", start_node=generate_sequence, description="Simple Sequence Summer")
g.add_edge(generate_sequence, sum_all_numbers)
g.add_edge(sum_all_numbers, dynamic_router)
g.route(dynamic_router, [squared, tripled])


client = create_client(in_process=True)
client.register_compute_graph(g)
```

#### 3: Invoke the Graph In-Process
```python
invocation_id = client.invoke_graph_with_object("sequence_summer", a=10)
result = client.graph_outputs("sequence_summer", invocation_id, "squared")
print(result)
```

You have built and ran a multi-stage workflow in-process! While running them in-process makes writing and testing Graphs easy, for production environments you would want an API to call them whenever there is data to process.

#### 4: Test your Graph as an API locally

Indexify server generates API endpoints for Compute Graphs, allowing external systems to invoke your workflows. The server can host multiple workflows and can execute functions across Graphs in parallel.

```bash
indexify-cli server-dev-mode
```

This starts the follwoing processes on your terminal -

**Indexify Server**: Manages state of graphs, orchestrates functions, and stores and distributes function outputs.

**Executor**: Runs python functions coordinates execution state of functions with server.

Change the code above to deploy the graph as an API -

```python
client = create_client() # Remove in_process=True
client.register_compute_graph(g) # Same as above

invocation_id = client.invoke_graph_with_object("sequence_summer", block_until_done=True, a=10)
result = client.graph_outputs("sequence_summer", invocation_id, "squared")
print(result)
```

This serializes your Graph code and uploads it to the server, and instantiates a new endpoint.

Everything else, remains the same in your application code that invokes the Graph to process data and retrieve outputs!

#### 5: Deploying Graph Endpoints using Docker Compose
You can spin up the server and executor using docker compose, and deploy and run in a production-like environment. Copy the [docker-compose.yaml file from here](https://raw.githubusercontent.com/tensorlakeai/indexify/refs/heads/main/docker-compose.yaml).

```bash
docker compose up
```

This starts the server and two replicas of the exeuctor in separate containers. Change the `replicas` field for the executor in docker compose to add more executors (i.e parallelism) to the workflow.

This uses a default executor container based on Debian and a vanilla Python installation.

#### 6: Building Executor Images with Custom Dependencies

You can build custom images for functions that require additional Python or System dependencies.

```python
from indexify import indexify_function, Image

image = (
    Image()
    .name("my-custom-image")
    .base_image("python:3.10.15-slim-bookworm")
    .tag("latest")
    .run("pip install indexify")
)

@indexify_function(image=image)
def func_a(x: int) -> str:
    ...
```
This will instruct the Indexify server to run the function in images with name `my-custom-image`.

Build the image -
```bash
indexify-cli build-image /path/to/workflow/code func_a
```

This will produce an image tagged `my-custom-image` by running all the commands specified above!

### Production Deployment

You could deploy Indexify Server in production in the following ways -

1. **Single Node:** Run both the server and executor on a single machine to run workflows on a single machine. Indexify would queue requests so you can upload as much data as you want, and can expect them to be eventually completed.

2. **Distributed:** If you anticipate processing a lot of data, and have fixed latency or throughput requirements you can deploy the executors on 100s or 1000s of machines for parallel execution. You can always scale up and down the clusters with ease.

#### Kubernetes Deployment
We have provided some basic Helm charts to deploy Indexify Server and Executors on Kubernetes. You can find them [here](https://github.com/tensorlakeai/indexify/tree/main/operations/k8s)

### Programming Model:

**Automatic Parallelization:**
If a function returns a list, downstream functions are invoked in parallel with each list element.
```python
@indexify_function()
def fetch_urls(num_urls: int) -> list[str]:
    return [
        'https://example.com/page1',
        'https://example.com/page2',
        'https://example.com/page3',
    ]

# scrape_page is called in parallel for every element of fetch_url across
# many machines in a cluster or across many worker processes in a machine
@indexify_function()
def scrape_page(url: str) -> str:
    # Code to scrape the page content
    content = requests.get(url).text
    return content
```
*Use Cases:* Generating Embedding from every single chunk of a document.

**Dynamic Routing**

Functions can route data to different nodes based on custom logic, enabling dynamic branching.

```python
@indexify_function()
def handle_error(text: str):
    # Logic to handle error messages
    pass

@indexify_function()
def handle_normal(text: str):
    # Logic to process normal text
    pass

# The function routes data into the handle_error and handle_normal based on the
# logic of the function.
@indexify_router()
def analyze_text(text: str) -> List[Union[handle_error, handle_normal]]:
    if 'error' in text.lower():
        return [handle_error]
    else:
        return [handle_normal]
```

*Use Cases:* Use Case: Processing outputs differently based on classification results.

**Reducing/Accumulating from Sequences:**
```python
@indexify_function()
def fetch_numbers() -> list[int]:
    return [1, 2, 3, 4, 5]

class Total(BaseModel):
    value: int = 0

@indexify_function(accumulate=Total)
def accumulate_total(total: Total, number: int) -> Total:
    total.value += number
    return total
```
*Use Cases:* Aggregating a summary from hundreds of web pages.

### What happens when you invoke a Compute Graph API?

* Indexify serializes the input and calls the API over HTTP.
* The server creates and schedules a Task for the fist function on an executor.
* The executor loads and executes the function and sends the data back to the server.
* The two above steps are repeated for every function in the Graph.

There are a lot of details we are skipping here! The scheduler utilizes a state of the art distributed and parallel event driven design internally to schedule tasks under 10 micro seconds, and uses many optimization techniques for storing blobs, rocksdb for state storage, streaming HTTP2 based RPC between server and executor to speed up execution.

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
