from tensorlake.functions_sdk.data_objects import File
from tensorlake.functions_sdk.functions import TensorlakeCompute

from inkwell.api.document import Document
from images import inkwell_image_gpu

class PDFParser(TensorlakeCompute):
    name = "pdf-parse"
    description = "Parser class that captures a pdf file"
    # Change to gpu_image to use GPU
    image = inkwell_image_gpu

    def __init__(self):
        super().__init__()
        from inkwell.pipeline import Pipeline

        self._pipeline = Pipeline()

    def run(self, file: File) -> Document:
        import tempfile

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".pdf") as f:
            f.write(file.data)
            document: Document = self._pipeline.process(f.name)
        return Document(pages=document.pages)
