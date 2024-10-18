<a name="readme-top"></a>
# Indexify 

![Tests](https://github.com/tensorlakeai/indexify/actions/workflows/test.yaml/badge.svg?branch=main)
[![Discord](https://dcbadge.vercel.app/api/server/VXkY7zVmTD?style=flat&compact=true)](https://discord.gg/VXkY7zVmTD)

## Create and Deploy Durable, Data-Intensive Agentic Workflows

*Indexify simplifies building and serving durable, multi-stage workflows as inter-connected Python functions and automagically deploys them as APIs.*

A **workflow** encodes data ingestion and transformation stages that can be implemented using Python functions. Each of these functions is a logical compute unit that can be retried upon failure or assigned to specific hardware.

> [!NOTE]  
> Indexify is the Open-Source core compute engine that powers Tensorlake's Serverless Workflow Engine for processing unstructured data. [Book a Demo to learn more about the platform.](https://calendly.com/diptanu/tensorlake-client-call)


### 💡 Use Cases

**Indexify** is a versatile data processing framework for all kinds of use-cases, including:

* [Scraping and Summarizing Websites](examples/website_audio_summary/)
* [Extracting and Indexing PDF Documents](examples/pdf_document_extraction/)
* [Transcribing and Summarizing Audio Files](examples/video_summarization/)
* [Object Detection and Description](examples/object_detection/)
* [Knowledge Graph RAG and Question Answering](examples/knowledge_graph/)

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

## 📚 A Minimal Example

Define a workflow by implementing its data transformation as composable Python functions. Functions decorated with `@indexify_function()`. These functions form the edges of a `Graph`, which is the representation of a compute graph. <br></br>
Functions serve as discrete units within a Graph, defining the boundaries for retry attempts and resource allocation. They separate computationally heavy tasks like LLM inference from lightweight ones like database writes. <br></br>
The example below is a pipeline that calculates the sum of squares for the first consecutive whole numbers. 

```python
from pydantic import BaseModel
from indexify import indexify_function, indexify_router, Graph
from typing import List, Union

# Returns a list of the first consecutive whole numbers
@indexify_function()
def generate_numbers(up_to: int) -> List[int]:
    return [i for i in range(up_to)]

# Returns the square of a given number
@indexify_function()
def square(i: int) -> int:
    return i ** 2

# Model to accumulate a running total
class Total(BaseModel):
    val: int = 0

# Adds a new number to the running total and returns its updated value
@indexify_function(accumulate=Total)
def add(total: Total, new: int) -> Total:
    total.val += new
    return total

# Constructs a compute graph connecting the three functions defined above into a workflow that generates
# a list of the first consecutive whole numbers, squares each of them, and calculates their cumulative sum
graph = Graph(name="sum_of_squares", start_node=generate_numbers, description="compute the sum of squares for each of the first consecutive whole numbers")
graph.add_edge(generate_numbers, square)
graph.add_edge(square, add)
```

[Read the Docs](https://docs.tensorlake.ai/quick-start) to learn more about how to test, deploy and create API endpoints for Workflows.


## 📖 Next Steps

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
