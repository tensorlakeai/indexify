from indexify import Image, RemoteGraph
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function

image = (
    Image(python="3.11")
    .name("tensorlake/pdf-blueprint-download")
    .run("pip install httpx")
)
@indexify_function(image=image)
def download_pdf(url: str) -> File:
    """
    Download pdf from url
    """
    import httpx
    resp = httpx.get(url=url, follow_redirects=True)
    resp.raise_for_status()
    return File(data=resp.content, mime_type="application/pdf")


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


if __name__ == "__main__":
    graph: Graph = create_graph()
    # Uncomment this to run the graph locally
    #invocation_id = graph.run(block_until_done=True, url="https://arxiv.org/pdf/2302.12854")
    import common_objects
    import images

    remote_graph = RemoteGraph.deploy(graph, additional_modules=[common_objects, images])
    invocation_id = remote_graph.run(
        block_until_done=True, url="https://arxiv.org/pdf/1706.03762"
    )
    print(f"Invocation ID: {invocation_id}")

    from chromadb  import HttpClient, QueryResult
    client = HttpClient(host="localhost", port=8000)
    from sentence_transformers import SentenceTransformer
    
    # Query VectorDB for similar text
    text_collection = client.get_collection("text_embeddings-1")
    query_embeddings = SentenceTransformer('Alibaba-NLP/gte-base-en-v1.5', trust_remote_code=True).encode(["transformers"])
    result: QueryResult = text_collection.query(query_embeddings, n_results=5, )
    documents = result["documents"][0]
    distances = result["distances"][0]
    for i, document in enumerate(documents):
        print(f"document {document}, score: {distances[i]}")
