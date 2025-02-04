import base64
from typing import Any, List

from tensorlake.functions_sdk.functions import TensorlakeCompute, tensorlake_function
from common_objects import ImageWithEmbedding, TextChunk, PDFParserDoclingOutput
from images import st_image

@tensorlake_function(image=st_image)
def chunk_text_docling(document: PDFParserDoclingOutput) -> List[TextChunk]:
    """
    Extract chunks from documents
    """
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    chunks = []

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
    for i, text in enumerate(document.texts):
        splits = text_splitter.split_text(text)
        for split in splits:
            chunks.append(TextChunk(chunk=split, page_number=i+1))

    return chunks


class TextEmbeddingExtractor(TensorlakeCompute):
    name = "text-embedding-extractor"
    description = "Extractor class that captures an embedding model"
    system_dependencies = []
    input_mime_types = ["text"]
    image = st_image

    def __init__(self):
        super().__init__()
        from sentence_transformers import SentenceTransformer
        self.model = SentenceTransformer('Alibaba-NLP/gte-base-en-v1.5', trust_remote_code=True)

    def run(self, input: TextChunk) -> TextChunk:
        embeddings = self.model.encode(input.chunk)
        input.embeddings = embeddings.tolist()
        return input


class ImageEmbeddingDoclingExtractor(TensorlakeCompute):
    name = "image-embedding-docling"
    description = "Extractor class that captures an embedding model"
    image=st_image

    def __init__(self):
        super().__init__()
        from sentence_transformers import SentenceTransformer
        self.model = SentenceTransformer("clip-ViT-B-32")

    def run(self, document: PDFParserDoclingOutput) -> List[ImageWithEmbedding]:
        import io
        from PIL import Image as PILImage

        embeddings = []
        for i, image_str in enumerate(document.images):
            img_bytes = io.BytesIO(base64.b64decode(image_str))
            img_bytes.seek(0)
            img_emb = self.model.encode(PILImage.open(img_bytes))
            img_bytes.seek(0)
            embeddings.append(
                ImageWithEmbedding(
                    embedding=img_emb,
                    image_bytes=img_bytes.getvalue(),
                    page_number=i+1,
                )
            )

        return embeddings
