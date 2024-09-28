# Indexify 

## Stateful Compute Framework for building Data-Intensive Agentic Workflows 

Indexify is a compute framework for building data-intensive workflows with agentic state machines. It lets you data transformation/extraction and business logic as Python functions and orchestrate data flow between them using graphs. The workflows are deployed as live API endpoints for seamless integration with existing systems. Some of the use-cases that you can use Indexify for - 

* Scraping and Summarizing websites 

* Document Extraction, Indexing and Populating Knowledge Graphs.

* Transcribing audio and summarization.

### Key Features
* **Dynamic Branching and Data Flow:** Supports dynamic dataflow branching across functions within a graph.
* **Local Inference:** Run multiple LLMs within workflow functions using LLamaCPP, vLLM, or Hugging Face Transformers by assigning functions to machines with adequate resources.
* **Distributed Map and Reduce:** Automatically parallelizes execution of functions over sequences across multiple machines. Reducer functions are called serially as map functions finish.
* **Version Graphs and Backfill:** Offers a backfill API to update already processed data when functions or models in graphs are updated.
* **Observability:** Provides a UI for visualizing and debugging complex dynamic graphs.
* **Placement Constraints:** Allows graphs to span GPU instances and cost-effective CPU machines, with functions assigned to specific instance types.
* **Request Queuing and Batching:** Automatically queues and batches parallel workflow invocations to maximize GPU utilization.

## Installation
```bash
pip install indexify
```

## Basic Usage

Create workflows by connecting Python functions into a graph. Each function is a logical compute unit that can be retried upon failure or assigned to specific hardware. For example, you can separate resource-intensive tasks like OpenAI API calls or local inference of LLMs from database write operations to avoid reprocessing data with models if a write fails. Indexify caches the output of every function, so when downstream processes are retried, previous steps aren’t rerun.

API calls to these graphs are automatically queued and routed based on the graph’s topology, eliminating the need for RPC libraries, Kafka, or additional databases to manage internal state and communication across different processes or machines.

### Example: Creating and Using a Compute Graph
```python
from pydantic import BaseModel

@indexify_function()
def generate_sequence(a: int) -> List[int]:
    return [i for i in range(a)]

class Sum(BaseModel):
    val: int
@indexify_function(init_value=Sum)
def sum_all_numbers(sum: Sum, val: int) -> Sum:
    return Sum(sum.val + val)

@indexify_function()
def square_of_sum(sum: Sum) -> int:
    return sum.val * sum.val

from indexify import ComputeGraph
g = ComputeGraph(name="sequence_summer", start_node=generate_sequence, description="Simple Sequence Summer")
g.add_edge(generate_sequence, sum_all_numbers)
g.add_edge(sum_all_numbers, square_of_sum)
```

#### Register and Invoke the Compute Graph 
```python
from indexify import create_client 
client = create_client(local=True)
client.register_compute_graph(g)

client.invoke_workflow_with_object("sequence_summer", a=10)
result = client.graph_outputs("sequence_summer", "square_of_sum")
print(result)
```

This is it! You have built and your first multi-stage workflow locally! 

#### Programming Model:

**Automatic Parallelization:**
If a function returns a list, downstream functions are invoked in parallel with each list element.
```python
@indexify_function()
def fetch_urls() -> list[str]:
    return [
        'https://example.com/page1',
        'https://example.com/page2',
        'https://example.com/page3',
    ]

@indexify_function()
def scrape_page(url: str) -> str:
    # Code to scrape the page content
    content = requests.get(url).text
    return content

@indexify_function()
def process_content(content: str) -> dict:
    # Process the content, e.g., extract data or summarize
    return {'summary': summarize(content)}
```
*Use Cases:* Generating Embedding from every single chunk of a document.

**Dynamic Routing**

Functions can route data to different nodes based on custom logic, enabling dynamic branching.
```python
@indexify_function()
def analyze_text(text: str) -> list:
    if 'error' in text.lower():
        return [handle_error]
    else:
        return [handle_normal]

@indexify_function()
def handle_error(text: str):
    # Logic to handle error messages
    pass

@indexify_function()
def handle_normal(text: str):
    # Logic to process normal text
    pass
```

*Use Cases:* Use Case: Processing outputs differently based on classification results.

**Reducing/Accumulating from Sequences:**

```python
@indexify_function()
def fetch_numbers() -> list[int]:
    return [1, 2, 3, 4, 5]

class Total(BaseModel):
    value: int = 0

@indexify_function(init_value=Total)
def accumulate_total(total: Total, number: int) -> Total:
    total.value += number
    return total
```
*Use Cases:* Aggregating a summary from hundreds of web pages.

### Deploying Graph as an API:

Indexify includes a server for deploying compute graphs as API endpoints, allowing external systems to invoke your workflows. The server can host multiple workflows and can execute functions across Graphs in parallel.

```bash
indexify-cli server-dev-mode
```

This starts the Indexify Server and an Executor on your terminal - 

**Indexify Server**: Manages state of graphs, orchestrates functions, and stores and distributes function outputs.

**Executor**: Runs python functions coordinates execution state of functions with server.

Change the code above to deploy the graph as an API on the server -

```python
client = create_client() # local=True
client.register_compute_graph(g) # Same as above
```

This serializes your Graph code and uploads it to the server, and instantiates a new endpoint.

Everything else, remains the same in your application code that invokes the Graph to process data and retrieve outputs! 

**What happens when you invoke a Compute Graph API?**

* Indexify serializes the input and calls the API over HTTP. 

* The server creates and schedules a Task for the fist function on an executor.

* The executor loads and executes the function and sends the data back to the server.

* The two above steps are repeated for every function in the Graph. 

There are a lot of details we are skipping here! The scheduler utilizes a state of the art distributed and parallel event driven design internally to schedule tasks under 10 micro seconds, and uses many optimization technoques around storing blobs, rocksdb for state storage, streaming HTTP2 based RPC between server and executor to speed up execution.

### Production Deployment  

You could deploy Indexify Server in production in the following ways -

1. **Single Node:** Run both the server and executor on a single machine to run workflows on a single machine. Indexify would queue requests so you can upload as much data as you want, and can expect them to be eventually completed.

2. **Distributed:** If you anticipate processing a lot of data, and have fixed latency or throughput requirements you can deploy the executors on 100s or 1000s of machines for parallel execution. You can always scale up and down the clusters with ease. 

### Roadmap

##### Scheduler 

* Enable batching in functions
* Data Local function executions - Prioritizs scheduling on machines where intermediate output lives for faster execution.
* Reducer optimizations - Being able to batch serial execution reduce function calls. 
* Machine parallel scheduling for even lower latency.
* Support Cycles in the graphs for more flexible agentic behaviors in Graphs.
* Ephemeral Graphs - multi-stage inference and retrieval with no persistence of intermediate outputs

##### SDK 

* Build a Typescript SDK for writing workflows in Typescript
