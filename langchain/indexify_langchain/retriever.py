from typing import Any, List, Optional

from langchain_core.documents import Document
from langchain_core.pydantic_v1 import root_validator
from langchain_core.retrievers import BaseRetriever

from langchain.callbacks.manager import CallbackManagerForRetrieverRun


class IndexifyRetriever(BaseRetriever):
    """`Indexify API` retriever.
    Example:

    client = IndexifyClient()
    params = {
        "repository_name":"default",
        "name": "embeddings",
        "top_k": 3
    }
    retriever = IndexifyRetriever(client=client,params=params)
    retriever.get_relevant_documents("sports")

    """

    client: Any
    """The Indexify client to use."""
    params: Optional[dict] = None
    """The parameters to pass to the Indexify client."""

    @root_validator(pre=True)
    def validate_client(cls, values: dict) -> dict:
        """Validate that the client is of the correct type."""
        from indexify.client import IndexifyClient

        if "client" in values:
            client = values["client"]
            if not isinstance(client, IndexifyClient):
                raise ValueError(
                    "Got unexpected client, should be of type indexify.client.IndexifyClient. "
                    f"Instead, got {type(client)}"
                )

        values["params"] = values.get("params", {})

        return values

    def _get_relevant_documents(
        self, query: str, *, run_manager: CallbackManagerForRetrieverRun
    ) -> List[Document]:
        repository = self.client.get_repository(self.params.get("repository_name"))
        args = {
            "top_k": self.params.get("top_k", 1),
            "query": query,
            "name": self.params["name"],
        }

        results = repository.search_index(**args)

        final_results = []
        for r in results:
            final_results.append(
                Document(page_content=r["text"], metadata=r["metadata"])
            )
        return final_results
