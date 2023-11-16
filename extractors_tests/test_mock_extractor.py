import unittest

from indexify_extractor_sdk.base_extractor import ExtractorWrapper, Content

from indexify_extractor_sdk.mock_extractor import MockExtractor, InputParams

class TestMockExtractor(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestMockExtractor, self).__init__(*args, **kwargs)

    def test_mock_extractor(self):
        params = InputParams(a=1, b="foo")
        e = MockExtractor()
        extracted_content = e.extract([Content(content_type="text", data="Hello World")], params)
        self.assertEqual(len(extracted_content), 1)
        self.assertEqual(len(extracted_content[0]), 3)


    def test_extractor_wrapper(self):
        e = ExtractorWrapper("indexify_extractor_sdk.mock_extractor:MockExtractor")
        extracted_content = e.extract([Content(content_type="text", data="Hello World")], '{"a": 1, "b": "foo"}')
        self.assertEqual(len(extracted_content), 1)
        self.assertEqual(len(extracted_content[0]), 3)

    def test_extractor_schema(self):
        e = ExtractorWrapper("indexify_extractor_sdk.mock_extractor:MockExtractor")
        schemas = e.schemas()
        self.assertEqual(schemas.embedding_schemas['embedding'].distance_metric, "cosine")

if __name__ == "__main__":
    unittest.main()
