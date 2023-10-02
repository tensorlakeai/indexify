import unittest
from content_loader_util import load_pdf_as_content

from indexify_extractors import PDFEmbedder

class TestPDFEmbedder(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestPDFEmbedder, self).__init__(*args, **kwargs)

    def test_pdf_embedder(self):
        content = load_pdf_as_content("extractors_tests/data/test.pdf")
        extractor = PDFEmbedder(max_context_length=512)
        embeddings = extractor.extract([content], {}) 
        self.assertEqual(len(embeddings), 28)
        self.assertEqual(len(embeddings[0].embeddings), 384)


if __name__ == "__main__":
    unittest.main()
