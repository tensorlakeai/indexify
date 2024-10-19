from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.indexify_functions import IndexifyFunction

from inkwell.api.document import Document
from images import inkwell_image_gpu

class PDFParser(IndexifyFunction):
    name = "pdf-parse"
    description = "Parser class that captures a pdf file"
    # Change to gpu_image to use GPU
    image = inkwell_image_gpu

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
