# Getting Started

We will build a simple RAG application in this demo where Indexify extracts content from text 


### Download the Indexify Binary
```shell
curl https://www.tensorlake.ai | sh
```

### Start the Service
```shell
./indexify server -d
```
This starts the Indexify ingestion API and scheduler. The server state, ingested and extract content will be stored on local disk. The following endpoints are started -

* Ingestion API - [http://localhost:8900](http://localhost:8900)
* User Interface - [http://localhost:8900/ui](http://localhost:8900/ui)

A internal scheduler endpoint is started at localhost:8950 for communicating with extractors.

### Install the client library
Indexify comes with Python and Typescript clients for ingesting unstructurd data and retreiving indexed content. These clients uses the HTTP APIs of Indexify under the hood.

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

=== "TypeScript"

    ```typescript
    client.addDocuments([
        "Indexify is amazing!",
        "Indexify is a retrieval service for LLM agents!",
        "Kevin Durant is the best basketball player in the world."
    ]);
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

## Install the Extractor SDK 
Extraction from unstructured data is done through Extractors. Install some extractors to get started. Open another shell, first install the extractors sdk.

```shell
pip install indexify-extractor-sdk
```

Now download and extractor from our hub. You can find the available extractors we have built on [Github](http://github.com/tensorlakeai/indexify-extractors).

```bash
indexify-extractor download hub://embedding/minilm-l6
```

Once the extractor SDK and extractors are downloaded, start and join them to the Indexify Control Plane.
```shell
indexify-extractor join minilm_l6:MiniLML6Extractor
```
The extractor is now ready to receive content you upload and extract embedding using the MiniLML6Extractor

#### Set up some Extraction Policies

The extraction policies informs Indexify how to extract information from ingested content. Extraction Policies allows you to specify the filters to match and the extractor to use for populating indexes.

=== "python"

    ```python
    client.add_extraction_policy(
        extractor="tensorlake/minilm-l6", 
        name="minilml6", 
        content_source="ingestion")

    extraction_policies = client.extraction_policies
    ```

=== "TypeScript"

    ```typescript
    client.addExtractionPolicy({
        extractor: "tensorlake/minilm-l6",
        name: "testpolicy",
        content_source: "ingestion"
    });
    
    const extractionPolicies = client.extractionPolicies;
    ```

=== "curl"

    ```shell
    curl -v -X POST http://localhost:8900/namespaces/default/extraction_policies \
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
from indexify_langchain import IndexifyRetriever
params = {"name": "minilml6.embedding", "top_k": 3}
retriever = IndexifyRetriever(client=client, params=params)
```

We are first setting up the IndexifyRetreiver that langchain is going to use.


```python
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI
```

Now, let's setup a chain with a prompt and use OpenAI to answer questions. Notice that we are passing the Indexfiy retreiver created above to get context for the RAG.
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
