# PDF Extraction

We are going to build a pipeline where we will upload research papers from arxiv.org and ask questions about them.

### Install the Indexify Extractor SDK, Langchain Retriever and the Indexify Client
```bash
pip install indexify-extractor-sdk indexify-langchain indexify
```

### Start the Indexify Server
```bash
indexify server -d
```

### Download an Embedding Extractor
On another terminal start the embedding extractor which we will use to index text from PDF documents.

=== "Shell"

    ```bash
    indexify-extractor download hub://embedding/minilm-l6
    indexify-extractor join minilm-l6.minilm_l6:MiniLML6Extractor
    ```
  
=== "Docker"

    ```shell
    docker run -d -v /tmp/indexify-blob-storage:/tmp/indexify-blob-storage tensorlake/minilm-l6 join --coordinator-addr=host.docker.internal:8950 --ingestion-addr=host.docker.internal:8900
    ```

### Download a PDF Data Extractor
Install ffmpeg on your machine 
=== "MacOS"
    ```bash
    pip install transformers accelerate
    ```
=== "Ubuntu"
    ```bash
    pip install transformers accelerate
    ```

On another terminal start a LayoutLM based PDF Data Extractor
=== "Shell"

    ```bash
    indexify-extractor download hub://pdf/layoutlm-document-qa
    indexify-extractor join layoutlm-document-qa.layoutlm_extractor:LayoutLMDocumentQA
    ```
=== "Docker"

    ```shell
    docker run -d -v /tmp/indexify-blob-storage:/tmp/indexify-blob-storage tensorlake/layoutlm-document-qa join --coordinator-addr=host.docker.internal:8950 --ingestion-addr=host.docker.internal:8900
    ```


### Create Extraction Policies
Instantiate the Indexify Client 
```python
from indexify import IndexifyClient
client = IndexifyClient()
```

First, create a policy to convert pdf to textual data.
```python
client.add_extraction_policy(extractor='tensorlake/layoutlm-document-qa', name="pdf-conversion")
```

Second, from the pdf's textual data create an embedding based index.
```python
client.add_extraction_policy(extractor='tensorlake/minilm-l6', name="embedding-generation", content_source="pdf-conversion")
```

### Upload an PDF File
```python
import requests
req = requests.get("https://arxiv.org/pdf/1910.13461.pdf")

with open('1910.13461.pdf','wb') as f:
    f.write(req.content)
```

```python
client.upload_file(path="1910.13461.pdf")
```

### What is happening behind the scenes
Indexify automatically reacts when ingestion happens and evaluates all the existing policies and invokes appropriate extractors for extraction. When the pdf to text extractor finishes getting textual data from the paper, it automatically fires off the embedding extractor to chunk and extract embedding and populate an index. 

You can upload 100s of pdf files parallely into Indexify and it will handle textual extraction and indexing the texts automatically. You can run many instances of the extractors for speeding up extraction, and Indexify's in-built scheduler will distribute the work transparently. 

### Perform RAG

Initialize the Langchain Retreiver.
```python
from indexify_langchain import IndexifyRetriever
params = {"name": "embedding-generation.embedding", "top_k": 3}
retriever = IndexifyRetriever(client=client, params=params)
```

Now create a chain to prompt OpenAI with data retreived from Indexify to create a simple Q and A bot
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
chain.invoke("What is Rajpurkar's SQuAD?")
```

```bash
"It is an extractive question answering task on Wikipedia paragraphs"
```