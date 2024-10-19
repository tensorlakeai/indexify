from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.indexify_functions import IndexifyFunction
from indexify import Image

from inkwell.api.document import Document

image = (
    Image(python="3.11")
    .name("tensorlake/pdf-blueprint-pdf-parser")
    .run("apt update")
    .run("apt install -y libgl1-mesa-glx git g++")
    .run("pip install torch")
    .run("pip install numpy")
    .run("pip install git+https://github.com/facebookresearch/detectron2.git@v0.6")
    .run("apt install -y tesseract-ocr")
    .run("apt install -y libtesseract-dev")
    .run("pip install \"py-inkwell[inference]\"")
)

gpu_image = (
    Image()
    .name("tensorlake/pdf-blueprint-pdf-parser-gpu")
    .base_image("pytorch/pytorch:2.4.1-cuda11.8-cudnn9-runtime")
    .run("apt update")
    .run("apt install -y libgl1-mesa-glx git g++")
    .run("pip install git+https://github.com/facebookresearch/detectron2.git@v0.6")
    .run("apt install -y tesseract-ocr")
    .run("apt install -y libtesseract-dev")
    .run("pip install \"py-inkwell[inference]\"")
)

class PDFParser(IndexifyFunction):
    name = "pdf-parse"
    description = "Parser class that captures a pdf file"
    # Change to gpu_image to use GPU
    image = gpu_image

    def __init__(self):
        super().__init__()
        from inkwell.pipeline import Pipeline

        self._pipeline = Pipeline()

    def run(self, input: File) -> Document:
        import tempfile

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".pdf") as f:
            f.write(input.data)
            document: Document = self._pipeline.process(f.name)
        return Document(pages=document.pages)
