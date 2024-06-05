# Basic RAG

Here we show an example of building a basic RAG application with Indexify. We are going to download content about Kobe Bryant and Steph Curry and ask questions about their careers. 

We will then delete content about Kobe from Indexify and see that we cannot get back any answers but queries about Steph still work.

### Install the Indexify Extractor SDK, Indexify Langchain and the Indexify Client
```bash
pip install indexify-extractor-sdk indexify indexify-langchain
```

### Start the Indexify Server
```bash
indexify server -d
```

### Download an Embedding Extractor
On another terminal start the embedding extractor which we will use to index text from the Wikipedia page.

=== "Shell"

    ```shell
    indexify-extractor download tensorlake/minilm-l6
    indexify-extractor join-server
    ```
=== "Docker"

    ```shell
    docker run -d -v /tmp/indexify-blob-storage:/tmp/indexify-blob-storage -p 9500:9500 tensorlake/minilm-l6 join-server --coordinator-addr=host.docker.internal:8950 --ingestion-addr=host.docker.internal:8900 --listen-port=9500
    ```


### Upload Content
We will use the langchain wikipedia loader to download content from wikipedia and upload to Indexify. We will also use langchain to prompt OpenAI for the RAG application.

```python
pip install --upgrade --quiet  wikipedia langchain_openai langchain-community
```

Now download some pages from Wikipedia and upload them to Indexify
```python
from langchain_community.document_loaders import WikipediaLoader
docs = WikipediaLoader(query="Kobe Bryant", load_max_docs=10).load()
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

Initialize the Langchain Retriever, create a chain to prompt OpenAI with data retrieved from Indexify to create a simple Q and A bot
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
Now ask any question about Kobe Bryant -
```python
  chain.invoke("When and where did Kobe Bryant win his championships?")
```

```bash
'Kobe Bryant won his championships in the years 2000, 2001, 2002, 2009, and 2010.'
```

This is obviously correct because Kobe is the GOAT.

Now, let's ask questions about Steph which the LLM won't be able to answer

```python
chain.invoke("When did Steph Curry win his championships?")
```

```bash
'There is no information provided in the context about Steph Curry winning championships. The context primarily focuses on Kobe Bryant and his career in basketball'
```

##  Content ID List
Use the code in the listing below to get the id's of the metadata related to Kobe so we can remove it later.

Now, let's add content about Steph Curry. Modify the code to use Steph Curry instead of Kobe Bryant in the wikipedia downloader

```python
from langchain_community.document_loaders import WikipediaLoader
docs = WikipediaLoader(query="Steph Curry", load_max_docs=10).load()
```

Try asking the question about Steph Curry again and you should see an accurate response

```bash
'Stephen Curry won his championships in 2015, 2017, 2018, and 2022'
```
##  Delete Content

Now, let's delete the content about Kobe Bryant

```python
  content_ids = ["df40ff9751b5573b", "c540d1f6ad8e274c", "ec13f2d6d907420e"] #  these are sample content ids, replace with your own
  client.delete_documents(content_ids)
```

Try asking the question about Kobe Bryant again now

```bash
'Kobe Bryant won his championships during his career with the Los Angeles Lakers, not in any of the provided context.'
```

However, asking about Steph should still work

```python3
  chain.invoke("When did Steph Curry win his championships?")
```

```bash
'Steph Curry won his championships in 2015, 2017, 2018, and 2022.'
```