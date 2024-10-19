from typing import Any, List

from indexify import Image
from indexify.functions_sdk.indexify_functions import IndexifyFunction, indexify_function
from sentence_transformers import SentenceTransformer
from common_objects import ImageWithEmbedding, TextChunk
from inkwell.api.document import Document
from inkwell.api.page import PageFragmentType
import base64

image = (
    Image(python="3.11")
    .name("tensorlake/pdf-blueprint-st")
    .run("pip install sentence-transformers")
    .run("pip install langchain")
    .run("pip install pillow")
    .run("pip install py-inkwell")
)

@indexify_function(image=image)
def chunk_text(document: Document) -> List[TextChunk]:
    """
    Extract chunks from document
    s"""
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
    chunks: List[TextChunk] = []
    for page in document.pages:
        page_text = ""
        for fragment in page.page_fragments:
            # Add the table or figure as a separate chunk, with the text extracted from OCR
            if (
                fragment.fragment_type == PageFragmentType.TABLE
                or fragment.fragment_type == PageFragmentType.FIGURE
            ):
                chunks.append(
                    TextChunk(chunk=fragment.content.text, page_number=page.page_number)
                )

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
    image = image

    def __init__(self):
        super().__init__()
        self.model = SentenceTransformer('Alibaba-NLP/gte-base-en-v1.5', trust_remote_code=True)

    def run(self, input: TextChunk) -> TextChunk:
        embeddings = self.model.encode(input.chunk)
        input.embeddings = embeddings.tolist()
        return input


class ImageEmbeddingExtractor(IndexifyFunction):
    name = "image-embedding"
    description = "Extractor class that captures an embedding model"
    image=image

    def __init__(self):
        super().__init__()
        self.model = SentenceTransformer("clip-ViT-B-32")

    def run(self, document: Document) -> List[ImageWithEmbedding]:
        from PIL import Image as PILImage
        import io
        embedding = []
        for page in document.pages:
            for fragment in page.page_fragments:
                if fragment.fragment_type == PageFragmentType.FIGURE:
                    img_bytes = io.BytesIO(base64.b64decode(fragment.content.image))
                    img_bytes.seek(0)
                    img_emb = self.model.encode(PILImage.open(img_bytes))
                    img_bytes.seek(0)
                    embedding.append(
                        ImageWithEmbedding(
                            embedding=img_emb,
                            image_bytes=img_bytes.getvalue(),
                            page_number=page.page_number,
                        )
                    )
        return embedding
