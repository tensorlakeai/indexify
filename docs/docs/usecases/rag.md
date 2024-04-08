# Basic RAG

Here we show an example of building a basic RAG application with Indexify. We are going to upload content about Kevin Durant from Wikipedia and ask questions about KD's career.

### Install the Indexify Extractor SDK, Indexify Langchain and the Indexify Client
```bash
pip install indexify-extractor-sdk indexify indexify-langchain
```

### Start the Indexify Server
```bash
indexify server -d
```

### Download an Embedding Extractor
On another terminal start the embedding extractor which we will use to index text from the wikiepdia page.

=== "Shell"

    ```shell
    indexify-extractor download hub://embedding/minilm-l6
    indexify-extractor join-server minilm-l6.minilm_l6:MiniLML6Extractor
    ```
=== "Docker"

    ```shell
    docker run -d -v /tmp/indexify-blob-storage:/tmp/indexify-blob-storage -p 9500:9500 tensorlake/minilm-l6 join-server --coordinator-addr=host.docker.internal:8950 --ingestion-addr=host.docker.internal:8900 --advertise-addr=0.0.0.0:9500
    ```


### Upload Content
We will use the lanchain wikipedia loader to download content from wikipedia and upload to Indexify. We will also use langchain to prompt OpenAI for the RAG application.

```python
pip install --upgrade --quiet  wikipedia langchain_openai langchain-community
```

Now download some pages from Wikipedia and upload them to Indexify
```python
from langchain_community.document_loaders import WikipediaLoader
docs = WikipediaLoader(query="Kevin Durant", load_max_docs=10).load()
```

Instantiate the Indexify Client 
```python
from indexify import IndexifyClient
client = IndexifyClient()
```

```python
for doc in docs:
    client.add_documents(doc.page_content)
```

### Create an Extraction Policy 
```python
client.add_extraction_policy(extractor='tensorlake/minilm-l6', name="wiki-embedding")
```

### Perform RAG

Create a retriever to feed in data from Indexify. 

```python
from indexify_langchain import IndexifyRetriever
params = {"name": "wiki-embedding.embedding", "top_k": 20}
retriever = IndexifyRetriever(client=client, params=params)
```

Initialize the Langchain Retreiver, create a chain to prompt OpenAI with data retreived from Indexify to create a simple Q and A bot
```python
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI
```

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
```
Now ask any question about KD -
```python
chain.invoke("When and where did KD win his championships?")
```

```bash
'Kevin Durant won his championships with the Golden State Warriors in 2017 and 2018.'
```


