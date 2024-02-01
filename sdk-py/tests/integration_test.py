import sys

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
        repository_name = str(uuid4())
        self.client.create_repository(repository_name)
        repository = self.client.get_repository(repository_name)
        assert repository.name == repository_name

    def test_add_documents(self):
        # Add single documents
        repository_name = str(uuid4())
        self.client.create_repository(repository_name)
        repository = self.client.get_repository(repository_name)

        repository.add_documents(
            Document(
                text="This is a test",
                labels={"source": "test"},
            )
        )

        # Add multiple documents
        repository.add_documents(
            [
                Document(
                    text="This is a new test",
                    labels={"source": "test"},
                ),
                Document(
                    text="This is another test",
                    labels={"source": "test"},
                ),
            ]
        )

        # Add single string
        repository.add_documents("test")

        # Add multiple strings
        repository.add_documents(["one", "two", "three"])

        # Add mixed
        repository.add_documents(["string", Document("document string", {})])

    def test_get_content(self):
        repository_name = str(uuid4())
        repo = self.client.create_repository(repository_name)
        repo.add_documents(["one", "two", "three"])
        content = repo.get_content()
        assert len(content) == 3

    def test_search(self):
        repository_name = str(uuid4())
        extractor_name = str(uuid4())

        print("create repository", repository_name)
        self.client.create_repository(repository_name)
        repository = self.client.get_repository(repository_name)
        url = "https://memory-alpha.fandom.com"

        print("bind extractor", extractor_name)
        repository.bind_extractor(
            extractor="tensorlake/minilm-l6",
            name=extractor_name,
            filters={"source": url},
        )

        repository.add_documents(
            [
                Document(
                    text="Indexify is also a retrieval service for LLM agents!",
                    labels={"url": url},
                )
            ]
        )
        time.sleep(10)
        results = repository.search_index(f"{extractor_name}.embedding", "LLM", 1)
        assert len(results) == 1

    def test_list_extractors(self):
        extractors = self.client.extractors()
        assert len(extractors) >= 1

    def test_create_repository(self):
        repository_name = str(uuid4())
        self.client.create_repository(repository_name)
        repository = self.client.get_repository(repository_name)
        assert repository.name == repository_name

    def test_bind_extractor(self):
        name = str(uuid4())
        repository_name = "binding-test-repository"
        self.client.create_repository(repository_name)
        repository = self.client.get_repository(repository_name)
        repository.bind_extractor(
            "tensorlake/minilm-l6",
            name,
        )

    def test_query_metadata(self):
        repository_name = str(uuid4())
        extractor_name = str(uuid4())
        self.client.create_repository(repository_name)
        repository = self.client.get_repository(repository_name)
        repository.bind_extractor(
            "tensorlake/minilm-l6",
            extractor_name,
        )

        for index in repository.indexes():
            index_name = index.get("name")
            repository.query_metadata(index_name)
            # TODO: validate response - currently broken

    def test_extractor_input_params(self):
        name = str(uuid4())
        repository_name = "binding-test-repository"
        self.client.create_repository(repository_name)
        repository = self.client.create_repository(repository_name)
        repository.bind_extractor(
            extractor="tensorlake/minilm-l6",
            name=name,
            input_params={
                "chunk_size": 300,
                "overlap": 50,
                "text_splitter": "char",
            },
        )

    def test_get_bindings(self):
        name = str(uuid4())
        repository = self.client.create_repository("binding-test-repository")
        repository.bind_extractor(
            "tensorlake/minilm-l6",
            name,
        )
        bindings = repository.extractor_bindings
        assert len(list(filter(lambda x: x.name.startswith(name), bindings))) == 1

    def test_get_indexes(self):
        name = str(uuid4())
        repository = self.client.create_repository("binding-test-repository")
        repository.bind_extractor(
            "tensorlake/minilm-l6",
            name,
        )
        indexes = repository.indexes()
        assert len(list(filter(lambda x: x.get("name").startswith(name), indexes))) == 1


if __name__ == "__main__":
    unittest.main()
