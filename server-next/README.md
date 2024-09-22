# Indexify 

#### Stateful Compute Engine for building and deploying LLM Workflows and Applications

Indexify is a stateful compute engine that empowers you to build and deploy multi-stage LLM workflows and applications as live API endpoints—using plain Python. 

* Write workflows **without handling state preservation** during retries, traffic overloads, or managing intermediate results.

* **Distribute** workflows across thousands of machines for parallel execution.

* **Place functions on appropriate hardware:** Run large model-invoking functions on GPUs while executing application logic or data fetching on cost-effective CPUs.

* **Automatic Batching:** Indexify automatically batches parallel workflow invocations and executes them on GPU machines, maximizing the utilization of your most valuable resources.

Workflows are structured as Graphs, enabling many interesting use-cases -

- **Document Processing and Indexing Pipelines**

- **Audio Transcription, Summarization and Indexing APIs**

- **Web Scraping and Structured Extraction Pipelines**

- **Multi-Stage-Retrieval APIs for Advanced RAG**


## Install 
```bash
pip install indexify
```

## Basic Usage 

Workflows are written as Python functions and laid out as Graphs. Functions in a Graph are automatically invoked when upstream functions finishes with their output.

API calls to workflows are automatically queued, failures are retried so you don't have to install any other tools like Kafka, SQS or databases and write code to manage state of your Graph API endpoints.

#### Write a Workflow 
```python
from pydantic import BaseModel

class Sum(BaseModel):
    val: int

@indexify_function(retries=3)
def generate_sequence(a: int) -> List[int]:
    return [i for i in range(a)]

@indexify_function(init_value=Sum)
def sum_all_numbers(sum: Sum, val: int) -> Sum:
    return Sum(sum.val + val)

@indexify_function(placement_constraints="python_ver>3.8 and os=linux")
def display(sum: Sum) -> str:
    return f"value of sequence: f{sum.val}"

from indexify import ComputeGraph
g = ComputeGraph(name="sequence_summer", start_node=generate_sequence, description="Simple Sequence Summer")
```

#### Invoke the Workflow 
```python
from indexify import create_client 
client = create_client(ephemeral=True)
client.invoke_workflow_with_object("sequence_summer", a=10)
result = client.graph_outputs("sequence_summer", "display")
print(result)
```

This is it! You have built your first multi-stage workflow, and ran it locally! 

#### Create a Graph Endpoint HTTP API  

Indexify comes with a server for deploying Compute Graphs as an HTTP API so they can be called from other applications or systems.

```bash
indexify-cli run-server
```

This starts the Indexify Server and an Executor - 

**Indexify Server**: Manages state of your graphs when they are called. It stores the outputs of the functions and calls them based on the structure of the graph. 

**Executor**: Runs the python functions which are part of your Graph.

Change the code above to deploy the graph as an API on the server -

```python
client = create_client(ephemeral=True)
```

Everything else, remains the same in your application code that invokes the Graph to process data! 

#### Distributed Execution by Design 

**Parallel Execution** - You can run as many executors you want on a single machine or on many 100s of machines. This will enable Indexify Server to run your graphs in parallel if they are invoked 1000s of times concurrently. 

**Placement Constraints** - You can make functions on the graph be executed on different classes of machines.

