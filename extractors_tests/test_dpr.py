import unittest
from indexify_extractors.dpr_onnx import OnnxDPR


class TestDPREmbeddings(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestDPREmbeddings, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        cls._dpr = OnnxDPR()

    def test_embedding_query(self):
        query = "What is the capital of France?"
        embeddings = self._dpr.generate_embeddings_query(query)
        self.assertEqual(len(embeddings), 768)

    def test_embedding_context(self):
        context = ["Paris is the capital of France.", "London is the capital of England."]
        embeddings = self._dpr.generate_embeddings_ctx(context)
        self.assertEqual(len(embeddings), 2)
        self.assertEqual(len(embeddings[0]), 768)


if __name__ == "__main__":
    unittest.main()
