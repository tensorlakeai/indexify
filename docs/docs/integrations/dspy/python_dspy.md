# DSPy 

Indexify complements DSPy by providing a robust platform for indexing large volume of multi-modal content such as PDFs, raw text, audio and video. It provides a retriever API to retrieve context for LLMs.

In this example, we demonstrate how a DSPy Retrieval Model (RM) can be created, and used like any other DSPy module. We create a simple custom Retriever Model that extends DSPy's base [Retrieve class](https://dspy-docs.vercel.app/docs/deep-dive/retrieval_models_clients/custom-rm-client). 

It will act as a wrapper that internally uses your indexify client for indexing and querying but externally leverages the modularity and design of DSPy.  


### Install the Indexify DSPy retriever package - 
```bash
pip install indexify-dspy
```

### Import the necessary libraries

```python
# Import the necessary libraries
import dspy

from typing import Optional, Union
from indexify import IndexifyClient
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


```python
class IndexifyRM(dspy.Retrieve):
    def __init__(
        self,
        indexify_client: IndexifyClient,
        k: int = 3,
    ):
        """Initialize the IndexifyRM."""
        self._indexify_client = indexify_client
        super().__init__(k=k)
```

Define the forward logic for retrieving and formatting the results. In our example, we take in a set of string queries, the index name to query documents from, and number of results to return(optional) and perform queries with our indexify client. 

```python
    def forward(
        self,
        query_or_queries: Union[str, list[str]],
        index_name: str,
        k: Optional[int]
    ) -> dspy.Prediction:

        queries = (
            [query_or_queries]
            if isinstance(query_or_queries, str)
            else query_or_queries
        )
        queries = [q for q in queries if q]  # Filter empty queries
        k = k if k is not None else self.k

        results = []
        for query in queries:
            response = self._indexify_client.search_index(
                index_name, 
                query, 
                k
            )
            results.extend(response)

        return dspy.Prediction(
            passages=[result["text"] for result in results],
        )
```


### Using the RM class 
```python
retrieve = IndexifyRM(indexify_client)
topk_passages = retrieve("Sports", "minilml6.embedding", k=2).passages
print(topk_passages)
```

You can use IndexifyRM like any other DSPy module or build your own wrapper for retrieval using the Indexify client following this example.  
