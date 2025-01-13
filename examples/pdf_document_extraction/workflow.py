from elastic_writer import ElasticSearchWriter
from embedding import chunk_text_docling, ImageEmbeddingDoclingExtractor
from tensorlake import RemoteGraph
from tensorlake.functions_sdk.data_objects import File
from tensorlake.functions_sdk.graph import Graph
from tensorlake.functions_sdk.functions import tensorlake_function
from images import http_client_image

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


# This graph is the alternate approach.
# This graph extracts text and image embeddings from the PDF using docling
# and writes them to ElasticSearch.
def create_graph_2() -> Graph:
    from embedding import TextEmbeddingExtractor
    from pdf_parser_docling import PDFParserDocling

    g = Graph(
        "Extract_pages_tables_images_pdf_docling",
        start_node=PDFParserDocling,
    )

    # Send the parse output to the text chunker and the image embedder.
    g.add_edge(PDFParserDocling, chunk_text_docling)
    g.add_edge(PDFParserDocling, ImageEmbeddingDoclingExtractor)

    ## Compute the text embedding vectors
    g.add_edge(chunk_text_docling, TextEmbeddingExtractor)

    ## Write text and image embeddings to vectordb
    g.add_edge(ImageEmbeddingDoclingExtractor, ElasticSearchWriter)
    g.add_edge(TextEmbeddingExtractor, ElasticSearchWriter)
    return g


if __name__ == "__main__":
    graph: Graph = create_graph_2()  # change this as required.

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
        graph,
        additional_modules=[common_objects, elasticsearch, images],
        server_url="http://localhost:8900",
    )

    invocation_id = remote_graph.run(
        block_until_done=True, file=file,
    )
    print(f"Invocation ID: {invocation_id}")