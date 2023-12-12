# Langhchain Integration for Indexify

Indexify complements LangChain by providing a robust platform for indexing large volume of multi-modal content such as PDFs, raw text, audio and video. It provides a retriever API to retrieve context for LLMs.

You can use our LangChain retriever from our repo located in `indexify_langchain/retriever.py` to begin retrieving your data.

Below is an example

```python
# setup your indexify client
from indexify.client import IndexifyClient
client = IndexifyClient()


# add docs
from indexify.repository import Document
repository = client.get_repository("default")

repository.add_documents(
    [
        Document(
            text="Indexify is amazing!",
            metadata={"url": "https://www.google.com"},
        ),
        Document(
            text="Indexify is a retrieval service for LLM agents!",
            metadata={"source": "test"},
        ),
        Document(
            text="Kevin Durant is the best basketball player in the world.",
            metadata={"source": "test"},
        ),
    ]
)


# implement retriever from indexify repo
from retriever import IndexifyRetriever

params = {"repository_name": "default", "index_name": "test", "top_k": 3}
retriever = IndexifyRetriever(client=client, params=params)

docs = retriever.get_relevant_documents("basketball")
```