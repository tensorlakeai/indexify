# Getting Started

We will build a few applications to demonstrate how to create LLM applications capable of making decisions or providing answers based on unstructured data. 

- Create a chat bot that can answer questions about NBA players based on information on Wikipedia.
- Ingest a [PDF file](https://arev.assembly.ca.gov/sites/arev.assembly.ca.gov/files/publications/Chapter_2B.pdf) related to income tax and get personalized analysis according to salary.
  
You will design declarative extraction graphs(real-time labeling and extraction data pipelines) that can automatically extract information from unstructured data. 

Indexify will automatically generate Vector Indexes if any embeddings are produced. Any structured data generated during extraction will be written into structured stores.

!!! note "Storage"
    Indexify uses LanceDB for storing embedding and sqlite3 for storing structured data, when it's run locally on laptops in the "dev" mode. You can use one of the many supported vector stores, and structured stores by specifying a custom configuration file.

### Download and Start Indexify Server

```shell
curl https://getindexify.ai | sh
```
Once the binary is downloaded, start the server in development mode.
```shell
./indexify server -d
```

This starts the Indexify ingestion API and scheduler. The server state, ingested and extract content will be stored on local disk. The following endpoints are started -

- Ingestion API ([http://localhost:8900](http://localhost:8900)) - The API endpoint for uploading content and retrieve from indexes and SQL Tables.
- User Interface ([http://localhost:8900/ui](http://localhost:8900/ui)) - Dashboard for extraction graphs, content and indexes.

!!! note ""
    A internal scheduler endpoint is started at localhost:8950 for communicating with extractors.

### Install the Extractor SDK

Extraction from unstructured data is done through Extractors. Install some extractors to get started. Open another shell, download some extractors.

!!! note "Tip"
    Create a virtualenv or miniconda environment to install any Python library you will be downloading and using to do this getting started exercise. 
    ```shell
    virtualenv ve
    source ve/bin/activate
    ```

```bash
pip install indexify-extractor-sdk
indexify-extractor download hub://embedding/minilm-l6
indexify-extractor download hub://text/chunking
indexify-extractor download hub://pdf/marker
```

Once the extractor SDK and extractors are downloaded, start and join them to the Indexify Control Plane. This is a long running process that extracts continuously when new data is ingested.

```bash
indexify-extractor join-server
```

### Install the client library

Indexify comes with Python and Typescript clients for ingesting unstructured data and retrieving indexed content. These clients use the HTTP APIs of Indexify under the hood.

=== "python"
    ```bash
    pip install indexify
    ```
=== "TypeScript"

    ```bash
    npm install getindexify
    ```

### Building the Wikipedia Chat Bot 
Building a chatbot is a three step process -

- Create an Extraction Graph to transform content into searchable vector indexes and structured data.
- Retrieve relevant information from the index, based on the question.
- Use an LLM to generate a response.

Create a file to write your application. In this tutorial we will create the extraction graph, ingestion and querying in a single application file, for real production usecases these aspects will probably live in separate components of your application.

=== "python"
    Create a file app.py
=== "typescript"
    Create a file app.ts

##### Create an Extraction Graph
Extraction Graphs allow you to create real time data pipelines that extract structured data or embeddings from unstructured data like documents or videos.

We create an extraction graph named `nbakb`. It instructs Indexify to do the following when any new content is added to this graph -

- Chunks texts.
- Runs them through an embedding model.
- Writes the embedding into a vector database.

=== "Python"
    ```python
    from indexify import IndexifyClient, ExtractionGraph 
    
    client = IndexifyClient()
    
    extraction_graph_spec = """
    name: 'nbakb'
    extraction_policies:
    - extractor: 'tensorlake/chunk-extractor'
        name: 'chunker'
        input_params:
            chunk_size: 1000
            overlap: 100
    - extractor: 'tensorlake/minilm-l6'
        name: 'wikiembedding'
        content_source: 'chunker'
    """

    extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
    client.create_extraction_graph(extraction_graph)                                            
    ```
=== "TypeScript"
    ```typescript
    import { IndexifyClient } from "getindexify";
    
    const client = await IndexifyClient.createClient();
    ```
At this point, if you visit the [UI](http://localhost:8900/ui) you will see an extraction graph being created.
![Extraction Graph](images/GS_ExtractionGraph.png)

#### Vector Indexes 
Extraction Graphs automatically creates and updates vector indexes if one or more extractors in the graph produces embedding as output. Indexify takes care of updating these indexes for you automatically when new embeddings are created. 

You can list all the vector indexes in a given namespace.

=== "python"
    ```python
    print(client.list_indexes())
    ```
=== "typescript"
    ```typescript
    ```

![alt text](images/GS_Vector_Indexes.png)
##### Adding Content
You can now add content to the extraction graph. Indexify will start running the graph whenever new content is added.
=== "python"
    ```python
    from langchain_community.document_loaders import WikipediaLoader
    docs = WikipediaLoader(query="Kevin Durant", load_max_docs=1).load()
    for doc in docs:
        client.add_documents("nbakb", doc.page_content)                 
    ```

=== "TypeScript"
    ```typescript
    ```

![alt text](images/GS_Content.png)

!!! note "Outcome"
    We now have an index, with texts from wikipedia chunked and embedded by MiniLML6.

##### RAG for Question Answering

We can use RAG to build the chatbot. We will retrieve data from the indexes, based on the question, and add them into the context of an LLM request to generate an answer. You can use any LLMs - OpenAI, Cohere, Anthropic or local models using LLama.cpp, Ollama or Hugginface.

```shell
pip install openai
```

Write a function that retrieves context for your RAG application
```python
def get_context(question: str, index: str, top_k=3):
    results = client.search_index(name=index, query=question, top_k=top_k)
    context = ""
    for result in results:
        context = context + f"content id: {result['content_id']} \n\n passage: {result['text']}\n"
    return context

def create_prompt(question, context):
    return f"Answer the question, based on the context.\n question: {question} \n context: {context}"
```

Thats pretty much all you need to create a Basic RAG application that relies only on vector indexes

You can now use LLM to generate responses based on questions and the retrieved context:

```python
from openai import OpenAI
client_openai = OpenAI()

question = "When and where did Kevin Durant win NBA championships?"
context = get_context(question, "nbakb.wikiembedding.embedding")
prompt = create_prompt(question, context)

chat_completion = client_openai.chat.completions.create(
    messages=[
        {
            "role": "user",
            "content": prompt,
        }
    ],
    model="gpt-3.5-turbo",
)
print(chat_completion.choices[0].message.content)
```
!!! note "Response"
    Kevin Durant won his championships with the Golden State Warriors in 2017 and 2018.

### Ingesting Income Tax PDF file for tax analysis on salary
In this example, we will ingest California's state tax guide, and make an LLM answer how much someone would be paying in taxes. While this example is simple, if you were building a production application on tax laws, you can ingest and extract information from 100s of such documents.

##### Extraction Graph Setup

Set up an extraction graph to process the PDF documents -

- Set the name of the extraction graph to "pdfqa".
- The first stage of the graph converts the PDF document into Markdown. We use the extractor `tensorlake/marker`, which uses a popular Open Source PDF to markdown converter model.
- The text is then chunked into smaller fragments. Chunking makes retreival and processing by LLMs efficient.
- The chunks are then embedded to make them searchable.
- Each stage has of the pipeline is named and connected to their upstream extractors using the field `content_source`

=== "Python"
    ```python
    from indexify import ExtractionGraph

    extraction_graph_spec = """
    name: 'pdfqa'
    extraction_policies:
    - extractor: 'tensorlake/marker'
        name: 'mdextract'
    - extractor: 'tensorlake/chunk-extractor'
        name: 'chunker'
        input_params:
            chunk_size: 1000
            overlap: 100
        content_source: 'mdextract'
    - extractor: 'tensorlake/minilm-l6'
        name: 'pdfembedding'
        content_source: 'chunker'
    """

    extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
    client.create_extraction_graph(extraction_graph)
    ```
=== "TypeScript"
    ```typescript
    ```
##### Document Ingestion

Add the PDF document to the "pdfqa" extraction graph
=== "Python"
    ```python
    import requests

    response = requests.get("https://arev.assembly.ca.gov/sites/arev.assembly.ca.gov/files/publications/Chapter_2B.pdf")
    with open("taxes.pdf", 'wb') as file:
        file.write(response.content)

    client.upload_file("taxes", "taxes.pdf")
    ```
=== "TypeScript"
    ```typescript
    ```
##### Prompting and Context Retrieval Function
We can use the same prompting and context retrieval function defined above to get context for the LLM based on the question.

=== "Python"
    ```python
    prompt = create_prompt(question, context)

    chat_completion = client_openai.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": prompt,
            }
        ],
        model="gpt-3.5-turbo",
    )
    print(chat_completion.choices[0].message.content)
    ```
=== "Typescript"
    ```typescript
    ```

!!! note "Response"
    Based on the provided information, the tax rates and brackets for California are as follows:

    - $0 - $11,450: 10% of the amount over $0
    - $11,450 - $43,650: $1,145 plus 15% of the amount over $11,450
    - $43,650 - $112,650: $5,975 plus 25% of the amount over $43,650
    - $112,650 - $182,400: $23,225 plus 28% of the amount over $112,650
    - $182,400 - $357,700: $42,755 plus 33% of the amount over $182,400
    - $357,700 and above: $100,604 plus 35% of the amount over $357,700

    For an income of $24,000, you fall within the $0 - $43,650 bracket. To calculate your tax liability, you would need to determine the tax owed on the first $11,450 at 10%, the tax owed on $11,450 - $24,000 at 15%, and add those together.

    Given that $24,000 falls within the $0 - $43,650 bracket, you would need to calculate the following:

    - Tax on first $11,450: $11,450 x 10% = $1,145
    - Tax on next $12,550 ($24,000 - $11,450): $12,550 x 15% = $1,882.50

    Therefore, your total tax liability would be $1,145 + $1,882.50 = $3,027.50.

### Next Steps
Now that you have learnt how to use Indexify, you can follow along to learn the following topics -

- Learn more about PDF, Video and Audio Extraction Use Cases.
- Integration with Langchain and DSPy if you use these frameworks.
- Deployment on Kubernetes
- Observability and understanding performance of Retreival and Extraction processes.