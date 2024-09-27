# Indexify 

## Stateful Compute Framework for building Data-Intensive Agentic Workflows 

Indexify is a compute framework for building data-intensive workflows with Agentic state machines. The workflows are deployed as live API endpoints for easy integration with existing systems. Application and Business logic are expressed as ***Functions*** while Dataflow across them are defined as ***Graphs***. Some of the use-cases that you can use Indexify for - 

* Scraping and Summarizing websites 

* Document Extraction, Indexing and Populating Knowledge Graphs.

* Transcribing audio and summarization.

### Key Features

Indexify gives you the following features out of the box,

* **Dynamic Dataflow**: Dynamic dataflow branching across ***Functions*** of a ***Graph***. 

* **Distributed Map and Reduce**: Parallelize execution of functions on sequences(map) automatically across many compute machines. Reducer functions are called serially as map functions finishes over time.

* **Version Graphs and Backfill**: Backfill API for updating already processed data when functions(or models) in graphs are updated.

* **Observability**: UI for visualization and debugging complex dynamic Graphs.

* **Placement Constraints**: Graphs can span across GPU instances and cost-effective CPU machines. Functions can be constrained to specific instance types. 

* **Request Queuing and Batching**: Automatically queue and batch parallel workflow invocations and maximize GPU utilization.

* **Output Checkpoints**: Automatically checkpoint intermediate outputs to quickly resume failed Graph stages.

## Install 
```bash
pip install indexify
```

## Basic Usage

Write your workflow as interconnected Python functions forming a graph. Each function acts as a logical unit that can be retried if it fails or designated to run on specific hardware. For example, when making OpenAI API calls or executing resource-intensive summarization models, you can split these tasks into separate functions to avoid repeating them if a subsequent database write fails. With Indexify, the output of every function is stored, so when downstream processes are retried, previous steps don't need to be rerun.

API calls to these Graphs are automatically queued and directed to the appropriate functions based on the graph's topology. This approach eliminates the need for RPC libraries, Kafka, or additional databases to manage internal state and communication between functions across different processes or machines in production environments.

### Create a Graph 
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

#### Automatic Parallelization 

When a function returns a `List` the downstream function is automatically called in parallel with all the elements of the list.

```python
def func_a(..) -> List[int]:
    pass

def func_b(x: int) -> SomeValue:
    pass
```

Here, `func_b` will be invoked in parallel with every output of `func_a`. There is no need to use Ray, Spark or Dask for parallelization when an upstream function produces a sequence of values.

**Use Cases:** Generating Embedding from every single chunk of a document.

#### Dynamic Routing 
You can add functions in the graph which don't produce new data but simply route them dynamically to one or more nodes in the Graph. This enables dynamic branching in a graph by executing some business logic on the input. 

```python
def func_a(..) -> SomeVal:
 pass

def do_X(val: SomeVal) -> SomeOtherVal:
  pass

def do_Y(val: SomeVal) -> SomeOtherVal1:
  pass

def route_between_X_Y(val: SomeVal) -> List[do_X, do_Y]:
  # Write your routing logic here and
  # pick either or all of the possible paths
  return [do_X]
```

**Example Use Cases:** Running classification tasks on the outputs of an upstream function, and processing it's output differently.

#### Reducing/Accumulating from Sequences

```python
@indexify_function
def func_a(num_val: int) -> List[int]:
    pass

class SomeValue:
    foo

@indexify_function(accumulate=SomeValue)
def func_b(val: SomeValue, x: int) -> SomeValue:
    pass
```

Here, `func_b` will be called serially with every value generated by `func_a`. The previous `SomeValue` produced by `func_b` will be injected in every subsequent calls. This allows to incrementally build state from a sequence generated by upstream functions.

**Use Cases:** - Producing a single summary from scraping 100s of web pages on a specific topic.

### Create a Graph Endpoint HTTP API  

Indexify comes with a server for deploying Compute Graphs as an API so they can be called from other applications or systems. The server can host multiple workflows and can execute functions across Graphs in parallel, and handles queuing, batching execution, managing function outputs.

```bash
indexify-cli run-server
```

This starts the Indexify Server and an Executor - 

**Indexify Server**: Manages state of your graphs when they are called. It stores the outputs of the functions and calls them based on the structure of the graph. 

**Executor**: Runs the python functions which are part of your Graph.

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

* The two above steps are executed for every function in the Graph. 

### Production Deployment  

You could deploy Indexify Server in production in the following ways -

1. **Single Node:** Run both the server and executor on a single machine to run workflows on a single machine. Indexify would queue requests so you can upload as much data as you want, and can expect them to be eventually completed.

2. **Distributed:** If you anticipate processing a lot of data, and have fixed latency or throughput requirements you can deploy the executors on 100s or 1000s of machines for parallel execution. You can always scale up and down the clusters with ease. 

### Roadmap

##### Scheduler 

* Enable batching in functions
* Data Local function executions - Move functions to where data lives for faster execution.
* Reducer optimizations - Being able to batch serial execution reduce function calls. 
* Machine parallel scheduling for even lower latency.
* Support Cycles in the graphs for more flexible agentic behaviors in Graphs.
* Ephemeral Graphs - multi-stage inference and retrieval with no persistence of intermediate outputs

##### SDK 

* Build a Typescript SDK for writing workflows in Typescript
