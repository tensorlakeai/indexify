import email.utils
from typing import List

import httpx

from . import DataLoader, FileMetadata


def convert_date_to_epoch(date_str: str) -> int:
    """
    Convert a date string from URL header to Unix epoch time.

    Args:
        date_str (str): The date string from the URL header.

    Returns:
        int: The Unix epoch time.
    """
    if not date_str:
        return 0
    parsed_date = email.utils.parsedate_to_datetime(date_str)
    return int(parsed_date.timestamp())


class UrlLoader(DataLoader):
    def __init__(self, urls: List[str], state: dict = {}):
        self.urls = urls

    def load(self) -> List[FileMetadata]:
        file_metadata_list = []
        for url in self.urls:
            response = httpx.head(url, follow_redirects=True)
            file_metadata_list.append(
                FileMetadata(
                    path=url,
                    file_size=response.headers.get("content-length", 0),
                    mime_type=response.headers.get("content-type"),
                    md5_hash="",
                    created_at=convert_date_to_epoch(response.headers.get("date")),
                    updated_at=convert_date_to_epoch(
                        response.headers.get("last-modified")
                    ),
                )
            )
        return file_metadata_list

    def read_all_bytes(self, file: FileMetadata) -> bytes:
        response = httpx.get(file.path, follow_redirects=True)
        return response.content

    def state(self) -> dict:
        return {}
