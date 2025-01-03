import unittest
from unittest.mock import Mock, patch

from indexify.data_loaders.url_loader import FileMetadata, UrlLoader


class TestUrlLoader(unittest.TestCase):
    @patch("indexify.data_loaders.url_loader.httpx.head")
    @patch("indexify.data_loaders.url_loader.httpx.get")
    def test_load(self, mock_get, mock_head):
        # Mock the HEAD request response
        mock_head_response = Mock()
        mock_head_response.headers = {
            "content-length": "1234",
            "content-type": "text/html",
            "date": "Wed, 21 Oct 2015 07:28:00 GMT",
            "last-modified": "Wed, 21 Oct 2015 07:28:00 GMT",
        }
        mock_head.return_value = mock_head_response

        # Mock the GET request response
        mock_get_response = Mock()
        mock_get_response.content = b"Hello, world!"
        mock_get.return_value = mock_get_response

        urls = ["http://example.com"]
        loader = UrlLoader(urls)
        file_metadata_list = loader.load()

        self.assertEqual(len(file_metadata_list), 1)
        file_metadata = file_metadata_list[0]
        self.assertEqual(file_metadata.path, "http://example.com")
        self.assertEqual(file_metadata.file_size, 1234)
        self.assertEqual(file_metadata.mime_type, "text/html")
        self.assertEqual(file_metadata.created_at, 1445412480)
        self.assertEqual(file_metadata.updated_at, 1445412480)

        # Test read_all_bytes method
        content = loader.read_all_bytes(file_metadata)
        self.assertEqual(content, b"Hello, world!")


if __name__ == "__main__":
    unittest.main()
