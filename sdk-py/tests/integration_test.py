from indexify.repository import Document, Repository, FilterBuilder
from indexify.client import IndexifyClient
import time

import unittest


class TestIntegrationTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIntegrationTest, self).__init__(*args, **kwargs)

    def test_list_repository(self):
        client = IndexifyClient()
        repositories = client.repositories()
        assert len(repositories) == 1

    def test_get_repository(self):
        client = IndexifyClient()
        repository = client.get_repository("default")
        assert repository.name == "default"

    def test_add_documents(self):
        # Add single documents
        client = IndexifyClient()
        repository = client.get_repository("default")
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
                )
            ]
        )

    def test_search(self):
        client = IndexifyClient()
        repository = client.get_repository("default")
        filter = (
            FilterBuilder().include("url", "foo.com").exclude("url", "bar.com").build()
        )
        repository.bind_extractor("MiniLML6", "searchindex", filter)
        repository.add_documents(
            [
                Document(
                    text="KD should have been the DPOY in 2017",
                    metadata={"source": "test"},
                )
            ]
        )
        time.sleep(5)
        results = repository.search_index("searchindex", "KD", 1)
        assert len(results) == 1

    def test_bind_extractor(self):
        client = IndexifyClient()
        repository = client.create_repository("binding-test-repository")
        filter = (
            FilterBuilder().include("url", "foo.com").exclude("url", "bar.com").build()
        )
        repository.bind_extractor("MiniLML6", "myindex", filter)


if __name__ == "__main__":
    unittest.main()
