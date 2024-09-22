# Indexify 

#### Stateful Compute Engine for building and deploying LLM Workflows and Applications


Indexify is a compute engine for building multi-stage workflows and applications and deploy them as Live API endpoints. The workflows can be run on multiple machines, making them very useful for AI/LLM workloads which often require running some models in functions along with some application logic or data fetching logic which can be run on CPUs.

Workflows can be laid out as Graphs, enabling many interesting use-cases -

- **Document Processing and Indexing Pipelines**

- **Audio Transcription, Summarization and Indexing APIs**

- **Web Scraping and Structured Extraction Pipelines**

- **Multi-Stage-Retrieval APIs for Advanced RAG**


## Install 
```bash
pip install indexify
```

## Basic Usage 

Before we jump into complex multi-stage workflows 

## Write a Workflow 
```python
from pydantic import BaseModel

class Sum(BaseModel):
    val: int

@indexify_function()
def generate_sequence(a: int) -> List[int]:
    return [i for i in range(a)]

@indexify_function(init_value=Sum)
def sum_all_numbers(sum: Sum, val: int) -> Sum:
    return Sum(sum.val + val)

@indexify_function()
def display(sum: Sum) -> str:
    return f"value of sequence: f{sum.val}"

from indexify import ComputeGraph
g = ComputeGraph(name="sequence_summer", start_node=generate_sequence, description="Simple Sequence Summer")
```

## Invoke the Workflow 
```python
from indexify import create_client 
client = create_client(ephemeral=True)
client.invoke_workflow_with_object("sequence_summer", a=10)
result = client.graph_outputs("sequence_summer", "display")
print(result)
```

This is it! You have built your first multi-stage workflow, and ran it locally! 

## Create a Graph Endpoint HTTP API  

Indexify comes with a server for deploying Compute Graphs as an HTTP API so they can be called from other applications or systems.

## Run Indexify Server 

```bash
indexify-cli run-server
```

This starts the Indexify Server and an Executor - 

**Indexify Server**: Manages state of your graphs when they are called. It stores the outputs of the functions and calls them based on the structure of the graph. 

**Executor**: Runs the python functions which are part of your Graph.

> You can run as many executors you want on a single machine or on many 100s of machines so that Indexify Server can run your graphs in parallel if you call the endpoints 1000s of times concurrently. 



