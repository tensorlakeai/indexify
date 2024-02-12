# Langhchain Integration for Indexify

Indexify complements LangChain by providing a robust platform for indexing large volume of multi-modal content such as PDFs, raw text, audio and video. It provides a retriever API to retrieve context for LLMs.

You can use our LangChain retriever from our repo located in `indexify_langchain/retriever.py` to begin retrieving your data.

Below is an example

```python
# setup your indexify client
from indexify.client import IndexifyClient
client = IndexifyClient()


# add docs
from indexify.client import Document

client.bind_extractor(
    "diptanu/minilm-l6-extractor",
    "minilm",
)

client.add_documents(
    [
        Document(
            text="Indexify is amazing!",
            metadata={"url": "https://github.com/tensorlakeai/indexify"},
        ),
        Document(
            text="Indexify is also a retrieval service for LLM agents!",
            metadata={"url": "https://github.com/tensorlakeai/indexify"},
        )
    ]
)


# implement retriever from indexify repo
from retriever import IndexifyRetriever

params = {"namespace": "default", "name": "minilm-embedding", "top_k": 3}
retriever = IndexifyRetriever(client=client, params=params)

docs = retriever.get_relevant_documents("indexify")
```