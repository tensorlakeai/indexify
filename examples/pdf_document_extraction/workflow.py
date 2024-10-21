from indexify import RemoteGraph
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function
from images import http_client_image
import httpx

@indexify_function(image=http_client_image)
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
def create_graph() -> Graph:
    from embedding import ImageEmbeddingExtractor, TextEmbeddingExtractor, chunk_text
    from chromadb_writer import ChromaDBWriter
    from pdf_parser import PDFParser

    g = Graph(
        "Extract_pages_tables_images_pdf",
        start_node=download_pdf,
    )

    # Parse the PDF which was downloaded
    g.add_edge(download_pdf, PDFParser)
    g.add_edge(PDFParser, chunk_text)

    ## Embed all the text chunks in the PDF
    g.add_edge(chunk_text, TextEmbeddingExtractor)
    g.add_edge(PDFParser, ImageEmbeddingExtractor)

    ## Write all the embeddings to the vector database
    g.add_edge(TextEmbeddingExtractor, ChromaDBWriter)
    g.add_edge(ImageEmbeddingExtractor, ChromaDBWriter)
    return g

# This graph extracts text and image embeddings from the PDF
# and writes them to the vector database
def create_graph_1() -> Graph:
    from embedding import ImageEmbeddingExtractor, TextEmbeddingExtractor, chunk_text
    from chromadb_writer import ChromaDBWriter
    from pdf_parser import PDFParser

    g = Graph(
        "Extract_pages_tables_images_pdf",
        start_node=PDFParser,
    )

    g.add_edge(PDFParser, chunk_text)

    ## Embed all the text chunks in the PDF
    g.add_edge(chunk_text, TextEmbeddingExtractor)
    g.add_edge(PDFParser, ImageEmbeddingExtractor)

    ## Write all the embeddings to the vector database
    g.add_edge(TextEmbeddingExtractor, ChromaDBWriter)
    g.add_edge(ImageEmbeddingExtractor, ChromaDBWriter)
    return g

if __name__ == "__main__":
    graph: Graph = create_graph_1()
    # Uncomment this to run the graph locally
    #invocation_id = graph.run(block_until_done=True, url="https://arxiv.org/pdf/2302.12854")
    import common_objects
    import images

    remote_graph = RemoteGraph.deploy(graph, additional_modules=[common_objects, images])

    file_url = "https://arxiv.org/pdf/1706.03762"
    import httpx
    resp = httpx.get(url=file_url, follow_redirects=True)
    resp.raise_for_status()
    file = File(data=resp.content, mime_type="application/pdf")
    
    invocation_id = remote_graph.run(
        block_until_done=True, file=file,
    )
    print(f"Invocation ID: {invocation_id}")

