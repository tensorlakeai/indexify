from embedding import chunk_text_docling, ImageEmbeddingDoclingExtractor, TextEmbeddingExtractor
from tensorlake import RemoteGraph
from tensorlake.functions_sdk.data_objects import File
from tensorlake.functions_sdk.graph import Graph
from tensorlake.functions_sdk.functions import tensorlake_function
from images import http_client_image
from elastic_writer import ElasticSearchWriter

@tensorlake_function(image=http_client_image)
def download_pdf(url: str) -> File:
    """
    Download pdf from url
    """
    import httpx
    resp = httpx.get(url=url, follow_redirects=True)
    resp.raise_for_status()
    return File(data=resp.content, mime_type="application/pdf")


# This graph, downloads a PDF, extracts text and image embeddings from the PDF
# and writes them to the vector database


from pdf_parser_docling import PDFParserDocling

extract_graph = Graph(
    "Extract_pages_tables_images_pdf_docling",
    start_node=PDFParserDocling,
)

# Send the parse output to the text chunker and the image embedder.
extract_graph.add_edge(PDFParserDocling, chunk_text_docling)
extract_graph.add_edge(PDFParserDocling, ImageEmbeddingDoclingExtractor)

## Compute the text embedding vectors
extract_graph.add_edge(chunk_text_docling, TextEmbeddingExtractor)

## Write text and image embeddings to vectordb
extract_graph.add_edge(ImageEmbeddingDoclingExtractor, ElasticSearchWriter)
extract_graph.add_edge(TextEmbeddingExtractor, ElasticSearchWriter)


if __name__ == "__main__":

    file_url = "https://arxiv.org/pdf/1706.03762"
    import httpx
    resp = httpx.get(url=file_url, follow_redirects=True)
    resp.raise_for_status()

    file = File(data=resp.content, mime_type="application/pdf")

    # uncomment to run locally
    #invocation_id = graph.run(block_until_true=True, file=file)
    #exit(0)
    import common_objects
    import images
    import elasticsearch  # this additional module is needed if you're using the second graph
    remote_graph = RemoteGraph.deploy(
        extract_graph,
        additional_modules=[common_objects, elasticsearch, images],
        server_url="http://localhost:8900",
    )
    
    invocation_id = remote_graph.run(
        block_until_done=True, file=file,
    )
    print(f"Invocation ID: {invocation_id}")