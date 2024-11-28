import os


def bytes_from_file(file_path: str) -> bytes:
    with open(file_path, "rb") as f:
        return f.read()


def write_to_file(file_path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(data)
