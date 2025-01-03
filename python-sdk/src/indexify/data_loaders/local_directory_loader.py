import os
from typing import List, Optional

from . import DataLoader, FileMetadata


class LocalDirectoryLoader(DataLoader):
    def __init__(
        self,
        directory: str,
        file_extensions: Optional[List[str]] = None,
        state: dict = {},
    ):
        self.directory = directory
        self.file_extensions = file_extensions
        self.processed_files = set(state.get("processed_files", []))

    def load(self) -> List[FileMetadata]:
        file_metadata_list = []
        for root, _, files in os.walk(self.directory):
            for file in files:
                if self.file_extensions is None or any(
                    file.endswith(ext) for ext in self.file_extensions
                ):
                    file_path = os.path.join(root, file)
                    if file_path not in self.processed_files:
                        file_metadata_list.append(FileMetadata.from_path(file_path))
                        self.processed_files.add(file_path)

        return file_metadata_list

    def read_all_bytes(self, file: FileMetadata) -> bytes:
        with open(file.path, "rb") as f:
            return f.read()

    def state(self) -> dict:
        return {"processed_files": list(self.processed_files)}
