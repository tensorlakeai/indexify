# Getting Started

We will build a few different extraction graphs(real time labelling and extraction data pipelines) and show you how to upload content, and build RAG applications from the extracted data.

- Create a chat bot that can answer questions about NBA players based on information on Wikiepdia
- Ingest a [PDF file](https://ak-static.cms.nba.com/wp-content/uploads/sites/4/2023/06/2023-NBA-Collective-Bargaining-Agreement.pdf) which contains ther rules that govern salary and player trading between teams.
- Ingest a recent podcast from Youtube and create a summary, and answer questions.
  
### Download and Start Indexify Server

```shell
curl https://www.tensorlake.ai | sh
```
Once the binary is downloaded, start the server.
```shell
./indexify server -d
```

This starts the Indexify ingestion API and scheduler. The server state, ingested and extract content will be stored on local disk. The following endpoints are started -

- Ingestion API ([http://localhost:8900](http://localhost:8900)) - The API endpoint for uploading content and retreive from indexes and SQL Tables.
- User Interface ([http://localhost:8900/ui](http://localhost:8900/ui)) - Dashboard for extraction graphs, content and indexes.

!!! note ""
    A internal scheduler endpoint is started at localhost:8950 for communicating with extractors.

### Install the Extractor SDK

Extraction from unstructured data is done through Extractors. Install some extractors to get started. Open another shell, download some extractors.

```bash
pip install indexify-extractor-sdk
indexify-extractor download hub://embedding/minilm-l6
indexify-extractor download hub://text/chunking
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
    ```python
    from indexify import IndexifyClient
    
    client = IndexifyClient()
    ```

=== "TypeScript"

    ```bash
    npm install getindexify
    ```
    ```typescript
    import { IndexifyClient } from "getindexify";
    
    const client = await IndexifyClient.createClient();
    ```
### Building the Chat Bot 
Building a chatbot is a three step process -
- Create an Extraction Graph to transform content into searchable vector indexes and structured data.
- Retrieve relevant information from the index, based on the question.
- Use an LLM to generate a response.

##### Create an Extraction Graph
Extraction Graphs allow you to create real time data pipelines that extract structured data or embeddings from unstructured data like documents or videos.

We create an extraction graph named `nbakb`. It first chunks texts, and then runs them through an embedding model, and finally writes the embedding into a vector database.

=== "Python"
    ```python
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
    ```


##### Adding Content

Download and add some pages about a few players.
=== "python"

    ```python
    from langchain_community.document_loaders import WikipediaLoader
    docs = WikipediaLoader(query="Kevin Durant", load_max_docs=1).load()
    for doc in docs:
        client.add_documents("sportsknowledgebase", doc.page_content)                 
    ```

=== "TypeScript"
    ```typescript
    ```


!!! note "Outcome"
    We now have an index, with texts from wikipedia chunked and embedded by MiniLML6.

##### RAG for Question Answering

We can use RAG to build the chatbot. We will retreive data from the indexes, based on the question, and add them into the context of an LLM request to generate an answer. You can use any LLMs - OpenAI, Cohere, Anthropic or local models using LLama.cpp, Ollama or Hugginface.

Get the name of the Indexes created by the extraction graph - 
```python
client.indexes()
```
!!! note "Response"
    ```python
    [{'name': 'sportsknowledgebase.wikiembedding.embedding',
    'embedding_schema': {'dim': 384, 'distance': 'cosine'}}]
    ```

```shell
pip install openai
```

Write a function that retreives context for your RAG application
```python
def get_context(question: str, index: str, top_k=3):
    results = client.search_index(name=index, query=question, top_k=3)
    context = ""
    for result in results:
        context = context + f"content id: {result['content_id']} \n\n passage: {result['text']}\n"
    return context

def create_prompt(question, context):
    return f"Answer the question, based on the question.\n question: {question} \n context: {context}"
```

Thats pretty much all you need to create a Basic RAG application that relies only on vector indexes

You can now use LLM to generate responses based on questions and the retreived context:

```python
from openai import OpenAI
client_openai = OpenAI()

prompt = create_prompt("When and where did Kevin Durant win NBA championships?")

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

