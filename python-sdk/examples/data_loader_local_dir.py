from indexify import IndexifyClient
from indexify.data_loaders import LocalDirectoryLoader

client = IndexifyClient()

# Load all files from a directory
director_loader = LocalDirectoryLoader(
    "/Users/diptanuc/Downloads", file_extensions=["pdf"]
)

# Load all files from a directory
content_ids = client.ingest_from_loader(director_loader, "rag_pipeline_mm")
print(content_ids)
