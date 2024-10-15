<a name="readme-top"></a>
# Indexify 

![Tests](https://github.com/tensorlakeai/indexify/actions/workflows/test.yaml/badge.svg?branch=main)
[![Discord](https://dcbadge.vercel.app/api/server/VXkY7zVmTD?style=flat&compact=true)](https://discord.gg/VXkY7zVmTD)

## Create and Deploy Durable, Data-Intensive Agentic Workflows

*Indexify simplifies building and serving durable, multi-stage workflows as inter-connected Python functions and automagically deploys them as APIs.*

### 💡 Use Cases

**Indexify** is a versatile data processing framework for all kinds of use-cases, including:

* [Scraping and Summarizing Websites](examples/website_audio_summary/)
* [Extracting and Indexing PDF Documents](examples/pdf_document_extraction/)
* [Transcribing and Summarizing Audio Files](examples/video_summarization/)
* [Knowledge Graph RAG and Question Answering](examples/knowledge_graph/)

### 📌 Concepts

A **workflow** represents a data transformation that can be implemented using Python functions. Each of these functions is a logical compute unit that can be retried upon failure or assigned to specific hardware. By modeling these functions as nodes of a graph and function composition as edges of one, the data transformation can be entirely described by an inter-connected graph known as a **compute graph**.

A **compute graph** implementing a workflow can be structured as either a **pipeline** or a **graph**.

* A **pipeline** represents a linear flow of data moving in a single direction.
* A **graph** represents a non-linear flow of data enabling parallel branches and conditional branching.

### ⭐ Key Features

* **Dynamic Routing:** Route data to different specialized models based on conditional branching logic.
* **Local Inference:** Execute LLMs directly within workflow functions using LLamaCPP, vLLM, or Hugging Face Transformers.
* **Distributed Processing:** Run functions in parallel across machines so that results across functions can be combined as they complete.
* **Workflow Versioning:** Version compute graphs to update previously processed data to reflect the latest functions and models.
* **Resource Allocation:** Span workflows across GPU and CPU instances so that functions can be assigned to their optimal hardware.
* **Request Optimization:** Maximize GPU utilization by automatically queuing and batching invocations in parallel.

## ⚙️ Installation

Install Indexify's SDK and CLI into your development environment:

```bash
pip install indexify
```

## 📚 Basic Usage

Get started with the basics of Indexify. This section covers how to define a compute graph in a Python script, test its behavior in the same process as its execution environment, and deploy it as an API to a local Indexify server for remote execution.

### 🛠️ 1: Define the Compute Graph

Start defining the workflow by implementing its data transformation as composable Python functions. Functions decorated with `@indexify_function() serve as discrete computational units within a Graph, defining the boundaries for retry attempts and resource allocation. These functions form the edges of a `Graph`, which is a representation of a compute graph in the Python SDK.

For instance, separating computationally heavy tasks like LLM inference from lightweight ones like database writes into distinct edges of the compute graph prevents repeating the inference if the write operation fails. 

The example below is a pipeline that calculates the sum of squares for the first consecutive whole numbers. Following a modular design by dividing the entire computation into composable functions enables Indexify to optimize the workflow's execution by storing each of its intermediate results.
Open up a new Python script and insert the following code:

```python
from pydantic import BaseModel
from indexify import indexify_function, indexify_router, Graph
from typing import List, Union

# Model to accumulate a running total
class Total(BaseModel):
    val: int = 0

# Adds a new number to the running total and returns its updated value
@indexify_function(accumulate=Total)
def add(total: Total, new: int) -> Total:
    total.val += new
    return total

# Returns a list of the first consecutive whole numbers
@indexify_function()
def generate_numbers(a: int) -> List[int]:
    return [i for i in range(a)]

# Returns the square of a given number
@indexify_function()
def square(i: int) -> int:
    return i ** 2

# Constructs a compute graph connecting the three functions defined above into a workflow that generates
# a list of the first consecutive whole numbers, squares each of them, and calculates their cumulative sum
graph = Graph(name="sum_of_squares", start_node=generate_numbers, description="compute the sum of squares for each of the first consecutive whole numbers")
graph.add_edge(generate_numbers, square)
graph.add_edge(square, add)
```

#### ✅ 2: Test the Compute Graph In-Process

Once the workflow has been defined, it's time to test its behavior to verify its correctness. For rapid prototyping, this can be performed within the same Python process as it's defined.

Append the following Python code to the script to calculate the sum of squares for the first 10 consecutive whole numbers:

```python
# Execute the workflow represented by the graph with an initial input of 10
invocation_id = graph.run(a=10)

# Get the execution's output by providing its invocation ID and the name of its last function
result = graph.output(invocation_id, "add")

# Display the result for verification
print(result)
```

Run the Python script 

```bash
python3 sum_of_squares.py
```

If the compute graph is defined, the script will execute the compute graph within the same process and return metadata about its completed functions: each its own name and output.

Verify that the displayed result is 285.

```bash
[Total(val=285)]
```

While testing a compute graph in-process provides a helpful tool for iterative development, an integration test where the compute graph is deployed as an API is more suited for a production environment.

#### 🌐 3: Deploy the Compute Graph as an API

An **Indexify server** hosts workflows represented by compute graphs and provides API endpoints for remote execution.

An **Indexify client** allows users to deploy and invoke workflows by calling API endpoints through the Python SDK.

Start an Indexify server using the CLI installed with the SDK:

```bash
indexify-cli server-dev-mode
```

The command above initializes the following processes in your terminal session:

* **Server:** Manages the state of compute graphs, orchestrates functions, and stores function outputs.
* **Executor:** Runs Python functions and coordinates execution state of functions with server.

The server can be accessed using the following URL: <http://localhost:8900/>.

Verify that it's currently running in your local environment by accessing its user interface using the following URL: <http://localhost:8900/ui/>.

The user interface presents a simple way to monitor the Indexify server for easy management, including its executors, compute graphs, and other resources.

Now that the server is running, update the Python script to deploy the compute graph to the server for remote execution and to return its output by making a call to its API endpoint through the Python SDK.

```python
from indexify import RemoteGraph

# Deploy the compute graph to the Indexify server for remote execution
remote_graph = RemoteGraph.deploy(graph)

# Uncomment the following line to reference a graph already deployed
# graph = RemoteGraph.by_name("sequence_summer") 

# Execute the workflow with an initial input of 10 to its first function
# Wait until the entire workflow has finished execution before returning
invocation_id = remote_graph.run(block_until_done=True, a=10)

# Get the execution's output through its API endpoint by providing its invocation ID and last function name
result = remote_graph.output(invocation_id, "add")

# Display the result for verification
print(result)
```

Run the modified Python script to serialize and upload the compute graph to the server, which then instantiates an API endpoint for future remote execution.

While the compute graph has an API endpoint for accessing its output as described above, the Python SDK manages it under the hood, returning the output given the invocation ID of the run and the name of the function. This ID uniquely identifies a specific execution of the compute graph while the name specifies the function within the execution whose output is to be returned.

```bash
python3 sum_of_squares.py
```

If the Indexify server is running, it will remotely execute the compute graph and return metadata about the completed tasks: each with its own task name, task ID, function name, invocation ID, executor ID, and outcome.

If an exception is raised while running the script, verify the server is accessible at `http://localhost:8900`. If there is an error accessing the server, make sure the server was started with the `indexify-cli server-dev-mode` command mentioned above.

Verify that the displayed result is 285.

```bash
[Total(val=285)]
```

The rest of the application code responsible for processing the data in the workflow remains unchanged!

## 📖 More Topics

* [Architecture of Indexify](https://docs.getindexify.ai/architecture)
* [Packaging Dependencies of Functions](https://docs.getindexify.ai/packaging-dependencies)
* [Programming Model](https://docs.getindexify.ai/key-concepts#programming-model)
* [Deploying Compute Graph Endpoints using Docker Compose](https://docs.getindexify.ai/operations/deployment#docker-compose)
* [Deploying Compute Graph Endpoints using Kubernetes](https://docs.getindexify.ai/operations/deployment#kubernetes)

### 🗺️ Roadmap

#### ⏳ Scheduler

* **Function Batching:** Process multiple functions in a single batch to improve efficiency.
* **Data Localized Execution:** Boost performance by prioritizing execution on machines where intermediate outputs exist already.
* **Reducer Optimizations:** Optimize performance by batching the serial execution of reduce function calls.
* **Parallel Scheduling:** Reduce latency by enabling parallel execution across multiple machines.
* **Cyclic Graph Support:** Enable more flexible agentic behaviors by leveraging cycles in graphs.
* **Ephemeral Graphs:** Perform multi-stage inference and retrieval without persisting intermediate outputs.
* **Data Loader Functions:** Stream values into graphs over time using the `yield` keyword.

#### 🛠️ SDK

* **TypeScript SDK:** Build an SDK for writing workflows in Typescript.

### Star History

[![Star History Chart](https://api.star-history.com/svg?repos=tensorlakeai/indexify&type=Date)](https://star-history.com/#tensorlakeai/indexify&Date)

### Contributors

<a href="https://github.com/tensorlakeai/indexify/graphs/contributors">
  <img alt="contributors" src="https://contrib.rocks/image?repo=tensorlakeai/indexify"/>
</a>

<p align="right" style="font-size: 14px; color: #555; margin-top: 20px;">
    <a href="#readme-top" style="text-decoration: none; color: #007bff; font-weight: bold;">
        ↑ Back to Top ↑
    </a>
</p>
