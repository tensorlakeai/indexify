import sys

from indexify import FilterBuilder
from indexify.repository import Document
from indexify.client import IndexifyClient
import time
from uuid import uuid4

import unittest


class TestIntegrationTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIntegrationTest, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        cls.client = IndexifyClient()

    def test_list_repository(self):
        repositories = self.client.repositories()
        assert len(repositories) >= 1

    def test_get_repository(self):
        repository = self.client.get_repository("default")
        assert repository.name == "default"

    def test_add_documents(self):
        # Add single documents
        repository = self.client.get_repository("default")
        repository.add_documents(
            Document(
                text="This is a test",
                metadata={"source": "test"},
            )
        )

        # Add multiple documents
        repository.add_documents(
            [
                Document(
                    text="This is a new test",
                    metadata={"source": "test"},
                ),
                Document(
                    text="This is another test",
                    metadata={"source": "test"},
                ),
            ]
        )

    def test_search(self):
        name = str(uuid4())
        repository = self.client.get_repository("default")
        url = "https://memory-alpha.fandom.com"
        filter = FilterBuilder().include("url", url).exclude("url", "bar").build()

        repository.bind_extractor(
            "diptanu/minilm-l6-extractor",
            name,
            filter=filter,
        )

        repository.add_documents(
            [
                Document(
                    text="Indexify is also a retrieval service for LLM agents!",
                    metadata={"url": url},
                )
            ]
        )
        time.sleep(10)
        results = repository.search_index(f"{name}-embedding", "LLM", 1)
        assert len(results) == 1

    def test_list_extractors(self):
        extractors = self.client.extractors()
        assert len(extractors) == 1

    def test_create_repository(self):
        repository_name = str(uuid4())
        self.client.create_repository(repository_name)
        repository = self.client.get_repository(repository_name)
        assert repository.name == repository_name

    def test_bind_extractor(self):
        name = str(uuid4())
        repository = self.client.create_repository("binding-test-repository")
        filter = (
            FilterBuilder().include("url", "foo.com").exclude("url", "bar.com").build()
        )
        repository.bind_extractor(
            "diptanu/minilm-l6-extractor",
            name,
            filter=filter,
        )

    def test_extractor_input_params(self):
        name = str(uuid4())
        repository = self.client.create_repository("binding-test-repository")
        repository.bind_extractor(
            extractor="diptanu/minilm-l6-extractor",
            name=name,
            input_params={
                "chunk_size": 300,
                "overlap": 50,
                "text_splitter": "char",
            },
        )

    def test_get_indexes(self):
        name = str(uuid4())
        repository = self.client.create_repository("binding-test-repository")
        repository.bind_extractor(
            "diptanu/minilm-l6-extractor",
            name,
        )
        indexes = repository.indexes()
        assert len(list(filter(lambda x: x.get("name").startswith(name), indexes))) == 1


if __name__ == "__main__":
    unittest.main()
