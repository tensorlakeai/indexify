# DSPy 

Indexify complements DSPy by providing a robust platform for indexing large volume of multi-modal content such as PDFs, raw text, audio and video. It provides a retriever API to retrieve context for LLMs.

We provide a Indexify Retreival Module for DSPy, that works with DSPy.

It will act as a wrapper that internally uses your indexify client for indexing and querying but externally leverages the modularity and design of DSPy.  


### Install the Indexify DSPy retriever package - 
```bash
pip install indexify-dspy indexify
```

### Import the necessary libraries

```python
import dspy
from indexify import IndexifyClient
from indexify_dspy import IndexifyRM

turbo = dspy.OpenAI(model="gpt-3.5-turbo")
indexify_client = IndexifyClient()
indexify_retriever_model = QdrantRM("index_name", indexify_client, k=3)

dspy.settings.configure(lm=turbo, rm=indexify_retriever_model)
```

Using the Retreival Model is very simple
```python
retrieve = dspy.Retrieve(k=3)
question = "Who are the NBA Finals MVPs"
topK_passages = retrieve(question).passages
```

### Create an indexify client and populate it with some documents

```python
indexify_client = IndexifyClient()
indexify_client.add_documents(
    [
        "Indexify is amazing!",
        "Indexify is a retrieval service for LLM agents!",
        "Steph Curry is the best basketball player in the world.",
    ],
)

indexify_client.add_extraction_policy(
    extractor="tensorlake/minilm-l6", name="minilml6",
    content_source="ingestion"
)
```

Initialize the IndexifyRM class

### Using the RM class 
```python
retrieve = IndexifyRM(indexify_client)
topk_passages = retrieve("Sports", "minilml6.embedding", k=2).passages
print(topk_passages)
```

### Setting up DSPy Module with Indexify

You can use IndexifyRM like any other DSPy module or build your own wrapper for retrieval using the Indexify client following this example.  

```python
class RAG(dspy.Module):
    def __init__(self, num_passages=2):
        super().__init__()

        self.retrieve = dspy.Retrieve(k=num_passages)
        ...

    def forward(self, question):
        context = self.retrieve(question).passages
        ...
``` 