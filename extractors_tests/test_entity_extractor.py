import unittest
from indexify_extractors import EntityExtractor, Content

class TestEntityExtractor(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestEntityExtractor, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        cls._entity_extractor = EntityExtractor()

    def test_extractor(self):
        input = Content(id="1", data="My name is Wolfgang and I live in Berlin")
        entities = self._entity_extractor.extract([input], {})
        self.assertEqual(len(entities), 2)
        self.assertEqual(
            entities[0].attributes,
            '{"entity": "Person", "value": "Wolfgang", "score": "0.98832768201828"}',
        )

    def test_output_schema(self):
        info = self._entity_extractor.info()
        self.assertIsNotNone(info.model_dump()["output_schema"]["attributes"])

if __name__ == "__main__":
    unittest.main()
