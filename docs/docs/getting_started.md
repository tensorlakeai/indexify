# Getting Started

We will build a simple RAG application in this demo where Indexify extracts content from text 


### Download the Indexify Binary
```shell
curl https://www.tensorlake.ai | sh
```

### Start the Service using Docker Compose
```shell
./indexify init-compose
docker compose up
```
This starts the Indexify server at port `8900` and additionally starts a Postgres server for storing metadata and storing embedding. We also start a basic embedding extractor which can chunk text and extract embedding from the chunks.

### Install the python client library
Indexify comes with a Python client. It uses the HTTP APIs of Indexify under the hood, and provide a convenient way of interacting with the server.
=== "python"

    ```shell
    pip install indexify
    ```
=== "python"

    ```python
    from indexify import IndexifyClient

    client = IndexifyClient()
    ```

### Adding Content

Indexify supports multiple ways of adding content through with it's API.

#### Add some documents

=== "python"

    ```python
    client.add_documents([
        "Indexify is amazing!",
        "Indexify is a retrieval service for LLM agents!",
        "Kevin Durant is the best basketball player in the world."
    ])
    ```
=== "curl"

    ```shell
    curl -v -X POST http://localhost:8900/namespaces/default/add_texts \
    -H "Content-Type: application/json" \
    -d '{"documents": [ 
            {"text": "Indexify is amazing!"},
            {"text": "Indexify is a retrieval service for LLM agents!"}, 
            {"text": "Kevin Durant is the best basketball player in the world."}
        ]}'
    ```

#### Set up some Extraction Policies

The extraction policies informs Indexify how to extract information from ingested content. Extraction Policies allows you to specify the filters to match and the extractor to use for populating indexes.

=== "python"

    ```python
    client.bind_extractor(extractor="tensorlake/minilm-l6", name="minilml6", content_source="ingestion")

    bindings = client.extractor_bindings
    ```

=== "curl"

    ```shell
    curl -v -X POST http://localhost:8900/namespaces/default/extractor_bindings \
    -H "Content-Type: application/json" \
    -d '{
            "extractor": "tensorlake/minilm-l6",
            "name": "minilml6"
        }'
    ```

We now have an index with embedding extracted by MiniLML6.

#### Let's build a Langchain based RAG with our extracted inforation

Next let's query the index created by the embedding extractor. The index will allow us to do semantic search over the text.

We will use Langchain to setup our RAG application and use Indexify as a retreiver to feed in data from Indexify.
```python
from retriever import IndexifyRetriever
params = {"name": "minilml6.embedding", "top_k": 3}
retriever = IndexifyRetriever(client=client, params=params)
```

Now, let's setup a chain with a prompt and use OpenAI to answer questions
```python
template = """Answer the question based only on the following context:
{context}

Question: {question}
"""
prompt = ChatPromptTemplate.from_template(template)

model = ChatOpenAI()

chain = (
    {"context": retriever, "question": RunnablePassthrough()}
    | prompt
    | model
    | StrOutputParser()
)
chain.invoke("who is the best basketball player in the world?")
```