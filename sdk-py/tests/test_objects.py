from indexify.repository import Repository
import json

import unittest


class TestParseObjects(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestParseObjects, self).__init__(*args, **kwargs)

    def test_parse_repository(self):
        json_objects = """
    {
      "repository": {
        "name": "default",
        "extractor_bindings": [
          {
            "extractor_name": "EntityExtractor",
            "index_name": "entities",
            "filters": [],
            "input_params": {}
          },
          {
            "extractor_name": "MiniLML6",
            "index_name": "star_trek_embeddings",
            "filters": [
              {
                "eq": {
                  "url": "https://memory-alpha.fandom.com/wiki/USS_Cayuga"
                }
              }
            ],
            "input_params": {}
          },
          {
            "extractor_name": "MiniLML6",
            "index_name": "embeddings",
            "filters": [],
            "input_params": {}
          }
        ],
        "metadata": {}
      }
    }
        """
        repo = Repository._from_json("https://svcurl", json.loads(json_objects))
        assert repo.name == "default"
        assert len(repo.extractor_bindings) == 3

        assert repo.extractor_bindings[0].extractor_name == "EntityExtractor"
        print(repo.extractor_bindings[1].filters[0])
        assert (
            repo.extractor_bindings[1].filters[0].includes["url"]
            == "https://memory-alpha.fandom.com/wiki/USS_Cayuga"
        )


if __name__ == "__main__":
    unittest.main()
