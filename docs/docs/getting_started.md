# Getting Started

In this tutorial, we'll show you how to create an online ingestion pipeline for Wikipedia pages, that performs - 

1. Structured Extraction(NER in this example) from the page using LLMs
2. Chunks, extract embeddings and write them into vector databases(LanceDB in this example).

You will also learn how to - 

1. Use GPT-4 or Mistral LLMs to answer questions using indexed information(RAG).
2. Retrieve the Named Entities extracted. 
3. Use the User Interface to visually debug your pipelines, and inspect the chunks to understand how pages are being broken down.

You'll need three different terminals open to complete this tutorial

1. Terminal 1 to download and run the Indexify Server
2. Terminal 2 to run our Indexify extractors which will handle structured extraction, chunking and embedding of ingested pages. 
3. Terminal 3 to run our python scripts to help load and query data from our Indexify server.

We'll indicate which terminal to run a command in by using the annotation

```bash title="( Terminal 1 ) Description Of Command"
<command goes here>
```

We can think of our Indexify server as a central coordinator, and data ingestion API. Extractors ( which we'll set up in a bit ) are specialized workers designed to perform a specific data processing task.

These tasks can range from embedding data, generating summaries or even automatically extracting features from unstructured data. All it takes to chain together these extractors into a complex pipeline is a **a single declarative `.yaml` file**.

## Indexify Server

Let's first start by downloading the indexify server and running it

```bash title="( Terminal 1 ) Download Indexify Server"
curl https://getindexify.ai | sh
./indexify server -d
```

By doing so, we immediately get the two following endpoints created.

| Endpoint       | Route                           | Description                                                                        |
| -------------- | ------------------------------- | ---------------------------------------------------------------------------------- |
| Ingestion API  | [/](http://localhost:8900)      | The API endpoint for uploading content and retrieving from indexes and SQL Tables. |
| User Interface | [/ui](http://localhost:8900/ui) | Dashboard for extraction graphs, content, and indexes.                             |



## Creating a Virtual Environment

??? note "Want the Source Code?"

    The source code for this tutorial can be found [here](https://github.com/tensorlakeai/indexify/tree/main/examples/getting_started/website) in our example folder

Let's start by creating a new virtual environment before installing the required packages in our virtual environment.

```bash title="( Terminal 2 ) Install Dependencies"
python3 -m venv venv
source venv/bin/activate
pip3 install indexify-extractor-sdk indexify wikipedia openai langchain_community
```

## Indexify Extractors

??? info "Extractors"

    Extractors help convert unstructured data into structured data or embeddings that we can query using a vector database or simple SQL.

Next, we'll need to download three extractors. Extractors are named, so that they can be referred in pipelines.
- `tensorlake/minilm-l6` for embedding text chunks.
- `tensorlake/chunking` for chunking our pages
- `tensorlake/openai` for entity extraction.

```bash title="( Terminal 2 ) Download Indexify Extractors"
source venv/bin/activate
indexify-extractor download tensorlake/openai
indexify-extractor download tensorlake/minilm-l6
indexify-extractor download tensorlake/chunk-extractor
```

??? note "OpenAI API KEY"
    The OpenAI extractor above requires setting the API key in the terminal. Don't forget to set it!

    export OPENAI_API_KEY=xxxxxx

We can then run all available extractors using the command below.

```python title="( Terminal 2 ) Starting Extractor Workers"
indexify-extractor join-server
```

## Loading in our data

### Defining Our Data Pipeline

Now that we've set up our `Indexify` server and extractors, it's time to define our data pipeline. We want the pipeline to take in text documents, split it into small chunks, extract entities and embed the chunks in parallel. 

??? info "Extraction Graphs"
    Extraction Graphs are multi-stage data-pipelines that transforms or extracts information from any type of data. They are called Extraction Graphs, because you can create branches in a single pipeline, so they can behave as graphs and not as linear sequence of stages.

??? info "Extraction Policy"
    Extraction Policy refers to an extractor and binds them into an Extraction Graph. They are also used to parameterize extractors which provide some knobs for configuring them slightly differently depending on use-cases.

We can do so using a simple `.yaml` file as seen below

```yaml title="graph.yaml"
name: "wiki_extraction_pipeline" #(1)!
extraction_policies:
  - extractor: "tensorlake/openai"
    name: "entity-extractor" 
    input_params:
      system_prompt: "Extract entities from text, and return the output in JSON format." #(5)!
  - extractor: "tensorlake/chunk-extractor"
    name: "chunker" #(2)!
    input_params:
      chunk_size: 1000 #(3)!
      overlap: 100
  - extractor: "tensorlake/minilm-l6"
    name: "wikiembedding"
    content_source: "chunker" #(4)!
```

1. Every data pipeline needs to have a unique name
2. Each extraction policy is uniquely identified by a single `name`
3. We can configure each extractor using the `input_params` field easily so that it is customized to our needs
4. We can chain together multiple extractors sequentially by specifying a `content_source` for each extractor.
5. The system prompts in LLM extractor passes in instructions to LLMs for structured extraction of the content being passed into the model.

Now that we've defined our pipeline using a `.yaml` file , let's see how we can create our first data pipeline in Indexify using our Indexify SDK.

```python title="setup.py"
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient() #(1)!

def create_extraction_graph():
    extraction_graph = ExtractionGraph.from_yaml_file("graph.yaml") #(2)!
    client.create_extraction_graph(extraction_graph) #(3)!

if __name__ == "__main__":
    create_extraction_graph()
```

1. We first create an instance of an `Indexify` client so that we can interact with Indexify server 
2. We then read in our `.yaml` file which we defined above and create an Extraction Graph using the yaml definition.
3. Lastly, we create our new data pipeline with the help of our `Indexify` client.

We can then run this code to create our new extraction graph. Once an Extraction Graph is created, it's exposed as an API on the server and starts running extractions whenever data is ingested into the system.

```bash title="( Terminal 3) Create Extraction Graph"
source venv/bin/activate
python3 ./setup.py
```
### Loading in Data

Now that we've written up a simple function to define our extraction graph, let's create a script to load in data from wikipedia into our new data pipeline.

```python title="ingest.py"
from indexify import IndexifyClient, ExtractionGraph
from langchain_community.document_loaders import WikipediaLoader

client = IndexifyClient()

def load_data(player):
    docs = WikipediaLoader(query=player, load_max_docs=1).load()

    for doc in docs:
        client.add_documents("wiki_extraction_pipeline", doc.page_content)

if __name__ == "__main__":
    load_data("Kevin Durant")
    load_data("Stephen Curry")
```

Now run this code to ingest data into Indexify. Indexify takes care of storing the data, and running the extraction policies in the graph reliably in parallel. 

```bash title="( Terminal 3) Ingest Data"
source venv/bin/activate
python3 ./ingest.py
```

## Query Indexify

You can query Indexify to - 

1. List ingested content by extraction graph. You can also list content per extraction policy.
2. Get extracted data from any of the extraction policies of an Extraction Graph.
3. Perform semantic search on vector indexes populated by embedding extractors.
4. Run SQL Queries on structured data(not in this tutorial).

Create a file `query.py` and add code to query Indexify -

#### Entities
We can get the list of entities extracted by the policy `entity-extractor` for every ingested page as -
```python  title="query.py"
from indexify import IndexifyClient

client = IndexifyClient()

ingested_content_list = client.list_content("wiki_extraction_pipeline") #(1)!
content_id = ingested_content_list[0].id
entities = client.get_extracted_content(
    content_id, 
    "wiki_extraction_pipeline", 
    "entity-extractor") #(2)!
```

1. Get a list of ingested content into the extraction graph.
2. Get the entities extracted by the `entity-extractor` extraction policy.

#### Chunks
Similarly, we can get the list of chunks created for one of the pages - 
```python   title="query.py"
chunks = client.get_extracted_content(
    content_id, 
    "wiki_extraction_pipeline", 
    "chunker") #(1)!
```

1. Get the entities extracted by the `chunker` extraction policy.

### Querying Vector Index 

Finally, lets perform semantic search on the embeddings created by the `wikiembedding` extraction policy.

```python title="query.py"
from openai import OpenAI

client_openai = OpenAI()

def query_database(question: str, index: str, top_k=3):
    retrieved_results = client.search_index(name=index, query=question, top_k=top_k) #(1)!
    context = "\n-".join([item["text"] for item in retrieved_results])
    response = client_openai.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": f"Answer the question, based on the context.\n question: {question} \n context: {context}",
            },
        ],
        model="gpt-3.5-turbo",
    )
    return response.choices[0].message.content


if __name__ == "__main__":
    index_name = "wiki_extraction_pipeline.wikiembedding.embedding"
    indexes = client.indexes()
    print(f"Vector indexes present: {indexes}, querying index: {index_name}")
    print(
        query_database(
            "What accomplishments did Kevin durant achieve during his career?",
            "wiki_extraction_pipeline.wikiembedding.embedding",
            4,
        )
    )
```

1. All we need to do to query our database is to use the `.search_index` method and we can get the top `k` elements which are closest in semantic meaning to the user's query.

When we run this file, we get the following output

```bash title="( Terminal 3 ) Run our RAG query"
>> OPENAI_API_KEY=<MY_API_KEY> python3 ./query.py
During his career, Kevin Durant has achieved numerous accomplishments, including winning two NBA championships, an NBA Most Valuable Player Award, two Finals MVP Awards, two NBA All-Star Game Most Valuable Player Awards, four NBA scoring titles, the NBA Rookie of the Year Award, and being named to ten All-NBA teams (including six First Teams). He has also been selected as an NBA All-Star 14 times and was named to the NBA 75th Anniversary Team in 2021. Additionally, Durant has won three gold medals in the Olympics as a member of the U.S. men's national team and gold at the 2010 FIBA World Championship
```

## Fault Tolerance and Reliability
Indexify is built for mission-critical use-cases, emphasizing reliability and scalability. It supports running thousands of extractors in parallel across thousands of compute nodes for horizontal scalability. If an extractor crashes, Indexify automatically retries the extraction on another node, ensuring a reliable extraction process. Here’s how it works in practice.

1. Open up a few more terminals and run the extractors.
2. Run the extractors in each of them. `indexify-extractor join-server` 
3. Upload 1000s of random texts, and watch Indexify load balance the work across all the extractor processes you have just started.
4. Kill one of the extractor process, and watch the extraction being retried on other running extractors.

## Next Steps

Now that you have learnt how to build a basic RAG application using Indexify, you can head over to learning more advanced topics

- [Learn how to extract text, tables and images from PDF documents](usecases/pdf_extraction.md).
- See how you can retrieve extracted data from [Langchain](integrations/langchain/python_langchain.md) or [DSPy](integrations/dspy/python_dspy.md).
- Deploying [Indexify server and extractors on Kubernetes](operations/kubernetes.md), when you are ready to take your app to production.
- [Observability](metrics.md) and understanding performance of Retrieval and Extraction processes.
