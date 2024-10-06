from typing import List

from indexify import Image, indexify_function
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.indexify_functions import IndexifyFunction

from common_objects import TextChunk, DocumentImage, DocumentImages
from inkwell import Pipeline, PageFragmentType, Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

image = (
    Image()
    .name("tensorlake/pdf-blueprint-pdf-parser")
    .run("apt update")
    .run("apt install -y libgl1-mesa-glx git g++")
    .run("pip install torch")
    .run("pip install numpy")
    .run("pip install langchain")
    .run("pip install git+https://github.com/facebookresearch/detectron2.git@v0.6")
    .run("apt install -y tesseract-ocr")
    .run("apt install -y libtesseract-dev")
    .run("pip install py-inkwell")
)

class PDFParser(IndexifyFunction):
    name = "pdf-parse"
    description = "Parser class that captures a pdf file"
    image = image

    def __init__(self):
        super().__init__()

        self._pipeline = Pipeline()

    def run(self, input: File) -> Document:
        import tempfile

        from inkwell import Document

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".pdf") as f:
            f.write(input.data)
            document: Document = self._pipeline.process(f.name)
        return Document(pages=document.pages)

@indexify_function(image=image)
def extract_chunks(document: Document) -> List[TextChunk]:
    """
    Extract chunks from document"""
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

@indexify_function(image=image)
def extract_images(document: Document) -> DocumentImages:
    """
    Extract images from document"""
    images = []
    for page in document.pages:
        for fragment in page.page_fragments:
            if fragment.fragment_type == PageFragmentType.FIGURE:
                image = DocumentImage(
                    image=fragment.content.image,
                    page_number=page.page_number,
                )
                images.append(image)
    return DocumentImages(images=images)
