from typing import List, Optional, Union

import httpx
from pydantic import BaseModel

from inkwell import Page, Document as InkwellDocument
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import (
    IndexifyFunction,
    indexify_function,
)
from indexify import create_client


@indexify_function()
def download_pdf(url: str) -> File:
    """
    Download pdf from url
    """
    resp = httpx.get(url=url, follow_redirects=True)
    resp.raise_for_status()
    return File(data=resp.content, mime_type="application/pdf")


class Document(BaseModel):
    pages: List[Page]


class PDFParser(IndexifyFunction):
    name = "pdf-parse"
    description = "Parser class that captures a pdf file"

    def __init__(self):
        super().__init__()
        from inkwell import Pipeline

        self._pipeline = Pipeline()

    def run(self, input: File) -> Document:
        import tempfile

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".pdf") as f:
            f.write(input.data)
            document: InkwellDocument = self._pipeline.process(f.name)
        return Document(pages=document.pages)


class TextChunk(BaseModel):
    chunk: str
    page_number: Optional[int] = None
    embeddings: Optional[List[float]] = None


@indexify_function()
def extract_chunks(document: Document) -> List[TextChunk]:
    """
    Extract chunks from document
    """
    from inkwell import PageFragmentType
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
    chunks: List[TextChunk] = []
    for page in document.pages:
        page_text = ""
        for fragment in page.page_fragments:
            # Add the table or figure as a separate chunk, with the text extracted from OCR
            if fragment.fragment_type == PageFragmentType.TABLE or fragment.fragment_type == PageFragmentType.FIGURE:
                chunks.append(TextChunk(chunk=fragment.content.text, page_number=page.page_number))

            # Add all the text from the page to the page text, and chunk them later.
            elif fragment.fragment_type == PageFragmentType.TEXT:
                page_text += fragment.content.text


        texts = text_splitter.split_text(page_text)
        for text in texts:
            chunk = TextChunk(chunk=text, page_number=page.page_number)
            chunks.append(chunk)
    return chunks


class TextEmbeddingExtractor(IndexifyFunction):
    name = "text-embedding-extractor"
    description = "Extractor class that captures an embedding model"
    system_dependencies = []
    input_mime_types = ["text"]

    def __init__(self):
        super().__init__()
        self.model = None

    def run(self, input: TextChunk) -> TextChunk:
        if self.model is None:
            from sentence_transformers import SentenceTransformer

            self.model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

        embeddings = self.model.encode(input.chunk)
        input.embeddings = embeddings.tolist()
        return input


class ImageWithEmbedding(BaseModel):
    embedding: List[float]
    page_number: int
    figure_number: int


class ImageEmbeddingExtractor(IndexifyFunction):
    name = "image-embedding"
    description = "Extractor class that captures an embedding model"

    def __init__(self):
        super().__init__()
        self.model = None

    def run(self, document: Document) -> List[ImageWithEmbedding]:
        from inkwell import PageFragmentType
        from sentence_transformers import SentenceTransformer

        if self.model is None:
            self.model = SentenceTransformer("clip-ViT-B-32")

        embedding = []
        for page in document.pages:
            for fragment in page.page_fragments:
                if fragment.fragment_type == PageFragmentType.FIGURE or fragment.fragment_type == PageFragmentType.TABLE:
                    image = fragment.content.image
                    img_emb = self.model.encode(image)
                    embedding.append(
                        ImageWithEmbedding(
                            embedding=img_emb,
                            page_number=page.page_number,
                            figure_number=0,
                        )
                    )
        return embedding


from lancedb.pydantic import LanceModel, Vector


class ImageEmbeddingTable(LanceModel):
    vector: Vector(512)
    page_number: int
    figure_number: int


class TextEmbeddingTable(LanceModel):
    vector: Vector(384)
    text: str
    page_number: int


class LanceDBWriter(IndexifyFunction):
    name = "lancedb_writer"

    def __init__(self):
        super().__init__()
        import lancedb
        import pyarrow as pa

        self._client = lancedb.connect("vectordb.lance")
        self._text_table = self._client.create_table(
            "text_embeddings", schema=TextEmbeddingTable, exist_ok=True
        )
        self._clip_table = self._client.create_table(
            "image_embeddings", schema=ImageEmbeddingTable, exist_ok=True
        )

    def run(self, input: Union[ImageWithEmbedding, TextChunk]) -> bool:
        if type(input) == ImageWithEmbedding:
            self._clip_table.add(
                [
                    ImageEmbeddingTable(
                        vector=input.embedding,
                        page_number=input.page_number,
                        figure_number=0,
                    )
                ]
            )
        elif type(input) == TextChunk:
            self._text_table.add(
                [
                    TextEmbeddingTable(
                        vector=input.embeddings,
                        text=input.chunk,
                        page_number=input.page_number,
                    )
                ]
            )
        return True


if __name__ == "__main__":
    g = Graph(
        "Extract_pages_tables_images_pdf",
        start_node=download_pdf,
    )

    # Parse the PDF which was downloaded
    g.add_edge(download_pdf, PDFParser)

    # Extract all the text chunks in the PDF
    # and embed the images with CLIP
    g.add_edges(PDFParser, [extract_chunks, ImageEmbeddingExtractor])

    ## Embed all the text chunks in the PDF
    g.add_edge(extract_chunks, TextEmbeddingExtractor)

    ## Write all the embeddings to the vector database
    g.add_edge(TextEmbeddingExtractor, LanceDBWriter)
    g.add_edge(ImageEmbeddingExtractor, LanceDBWriter)

    client = create_client(in_process=True)
    client.register_compute_graph(g)
    invocation_id = client.invoke_graph_with_object(
        g.name, block_until_done=True, url="https://arxiv.org/pdf/2106.00043.pdf"
    )
    print(f"Invocation ID: {invocation_id}")

    


    ## After extraction, lets test retreival

    import lancedb
    import sentence_transformers

    client = lancedb.connect("vectordb.lance")
    text_table = client.open_table("text_embeddings")
    st = sentence_transformers.SentenceTransformer(
        "sentence-transformers/all-MiniLM-L6-v2"
    )
    emb = st.encode("Generative adversarial networks")
    results = text_table.search(emb.tolist()).limit(10).to_pydantic(TextEmbeddingTable)
    print(f"Found {len(results)} results")
    for result in results:
        print(f"page_number: {result.page_number}\n\ntext: {result.text}")
