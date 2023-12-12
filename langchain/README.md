# Langhchain Integration for Indexify

Indexify complements LangChain by providing a robust platform for indexing and retrieving large volumes of text efficiently. It's particularly useful in scenarios where the LLM needs to sift through extensive documents quickly to find relevant information.

You can use our LangChain retriever from our repo located in `indexify_langchain/retriever.py` to begin retrieving your data.

Below is an example

```python
# setup your indexify client
from indexify.client import IndexifyClient
client = IndexifyClient()

# implement retriever from indexify repo
from retriever import IndexifyRetriever

params = {"repository_name": "default", "index_name": "state_of_the_union", "top_k": 9}
retriever = IndexifyRetriever(client=client, params=params)

docs = retriever.get_relevant_documents("us president")
```