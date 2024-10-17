from indexify import Image, RemoteGraph
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function

image = (
    Image(python="3.10")
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
    from embedding import ImageEmbeddingExtractor, TextEmbeddingExtractor
    from lancedb_functions import LanceDBWriter
    from pdf_parser import PDFParser, extract_chunks, extract_images

    g = Graph(
        "Extract_pages_tables_images_pdf",
        start_node=download_pdf,
    )

    # Parse the PDF which was downloaded
    g.add_edge(download_pdf, PDFParser)

    # Extract all the text chunks in the PDF
    # and embed the images with CLIP
    g.add_edges(PDFParser, [extract_chunks, extract_images])

    ## Embed all the text chunks in the PDF
    g.add_edge(extract_chunks, TextEmbeddingExtractor)
    g.add_edge(extract_images, ImageEmbeddingExtractor)

    ## Write all the embeddings to the vector database
    g.add_edge(TextEmbeddingExtractor, LanceDBWriter)
    g.add_edge(ImageEmbeddingExtractor, LanceDBWriter)
    return g


if __name__ == "__main__":
    #graph: Graph = create_graph()
    ## Uncomment this to run the graph locally
    ##invocation_id = graph.run(url="https://arxiv.org/pdf/2106.00043.pdf")
    #import common_objects

    #remote_graph = RemoteGraph.deploy(graph, additional_modules=[common_objects])
    #invocation_id = remote_graph.run(
    #    block_until_done=True, url="https://arxiv.org/pdf/2106.00043.pdf"
    #)
    #print(f"Invocation ID: {invocation_id}")

    # After extraction, lets test retreival

    import lancedb
    import sentence_transformers

    client = lancedb.connect("vectordb.lance")
    text_table = client.open_table("text_embeddings")
    st = sentence_transformers.SentenceTransformer(
       "sentence-transformers/all-MiniLM-L6-v2"
    )
    emb = st.encode("Generative adversarial networks")

    from lancedb.pydantic import LanceModel, Vector
    class TextEmbeddingTable(LanceModel):
            vector: Vector(384)
            text: str
            page_number: int

    results = text_table.search(emb.tolist()).limit(10).to_pydantic(TextEmbeddingTable)
    print(f"Found {len(results)} results")
    for result in results:
       print(f"page_number: {result.page_number}\n\ntext: {result.text}")
