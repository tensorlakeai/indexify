
import unittest
from indexify_extractors import LanguageExtractor, Content

class TestLanguageExtractor(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestLanguageExtractor, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        cls._entity_extractor = LanguageExtractor()

    def test_extractor(self):
        input = Content(id="1", data="My name is Wolfgang and I live in Berlin")
        languages = self._entity_extractor.extract([input], {})
        self.assertEqual(len(languages), 1)
        self.assertEqual(
            languages[0].attributes,
            '{"entity": "language", "value": "en", "score": "1.0"}',
        )

    def test_output_schema(self):
        info = self._entity_extractor.info()
        self.assertIsNotNone(info.model_dump()["output_schema"]["attributes"])

if __name__ == "__main__":
    unittest.main()
