import io
from typing import List, Optional, Union

import httpx
from pydantic import BaseModel

from indexify.extractors.pdf_parser import Page, PageFragmentType, PDFParser
from indexify.functions_sdk.data_objects import BaseData, File
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import (
    IndexifyFunction,
    indexify_function,
)
from indexify.local_runner import LocalRunner


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


@indexify_function()
def parse_pdf(file: File) -> Document:
    """
    Parse pdf file and returns pages:
    """
    parser = PDFParser(file.data)
    pages: List[Page] = parser.parse()
    return Document(pages=pages)


class TextChunk(BaseData):
    chunk: str
    metadata: dict = {}
    page_number: Optional[int] = None
    embeddings: Optional[List[float]] = None


@indexify_function()
def extract_chunks(document: Document) -> List[TextChunk]:
    """
    Extract chunks from document
    """
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000)
    chunks: List[TextChunk] = []
    for page in document.pages:
        for fragment in page.fragments:
            if fragment.fragment_type == PageFragmentType.TEXT:
                texts = text_splitter.split_text(fragment.text)
                for text in texts:
                    chunks.append(TextChunk(chunk=text, page_number=page.number))

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
    system_dependencies = []
    input_mime_types = ["text"]

    def __init__(self):
        super().__init__()
        self.model = None

    def run(self, document: Document) -> List[ImageWithEmbedding]:
        from PIL import Image
        from sentence_transformers import SentenceTransformer

        if self.model is None:
            self.model = SentenceTransformer("clip-ViT-B-32")

        embedding = []
        for page in document.pages:
            for fragment in page.fragments:
                if fragment.fragment_type == PageFragmentType.FIGURE:
                    image = fragment.image
                    img_emb = self.model.encode(Image.open(io.BytesIO(image.data)))
                    embedding.append(
                        ImageWithEmbedding(
                            embedding=img_emb,
                            page_number=page.number,
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
    g.add_edge(download_pdf, parse_pdf)

    # Extract all the text chunks in the PDF
    # and embed the images with CLIP
    g.add_edges(parse_pdf, [extract_chunks, ImageEmbeddingExtractor])

    ## Embed all the text chunks in the PDF
    g.add_edge(extract_chunks, TextEmbeddingExtractor)

    ## Write all the embeddings to the vector database
    g.add_edge(TextEmbeddingExtractor, LanceDBWriter)
    g.add_edge(ImageEmbeddingExtractor, LanceDBWriter)

    local_runner = LocalRunner()
    local_runner.run_from_serialized_code(
        code=g.serialize(),
        url="https://raft.github.io/raft.pdf",
    )

    ## After extraction, lets test retreival

    import lancedb
    import sentence_transformers

    client = lancedb.connect("vectordb.lance")
    text_table = client.open_table("text_embeddings")
    st = sentence_transformers.SentenceTransformer(
        "sentence-transformers/all-MiniLM-L6-v2"
    )
    emb = st.encode("leader election")
    results = text_table.search(emb.tolist()).limit(10).to_pydantic(TextEmbeddingTable)
    print(f"Found {len(results)} results")
    for result in results:
        print(f"page_number: {result.page_number}\n\ntext: {result.text}")
