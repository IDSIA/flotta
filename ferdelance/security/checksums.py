from hashlib import sha256
from pathlib import Path


def str_checksum(
    content: str | bytes,
    encoding: str = "utf8",
) -> str:
    if isinstance(content, str):
        content = content.encode(encoding)

    checksum = sha256()
    checksum.update(content)

    return checksum.hexdigest()


def file_checksum(path: Path, CHUNK_SIZE: int = 4096) -> str:
    checksum = sha256()

    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            checksum.update(chunk)

    return checksum.hexdigest()
