"""Tool for memory search."""
from indexify.indexify import Indexify, Metric, TextSplitter
from pydantic import BaseModel

class IndexifySearchTool(BaseModel):
    """Search memory embeddings for information."""

    index: Indexify

    def __init__(self, index_name: str = "my_index", indexify_url="http://localhost:8900") -> None:
        """Initialize the tool."""
        self.index = Indexify.get_index(index_name, indexify_url)
        
    def run(self, query: str,  top_k: int = 10) -> str:
        return self.index.search(query, top_k)
    