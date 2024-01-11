from hashlib import sha256
from pathlib import Path


def str_checksum(
    content: str | bytes,
    encoding: str = "utf8",
) -> str:
    """Returns the checksum of a the given content. The checksum is done
    using the sha256 algorithm.

    Args:
        content (str | bytes):
            The content to calculate the checksum for.
        encoding (str, optional):
            The encoding of the string.
            Defaults to "utf8".

    Returns:
        str:
            The checksum value for the given content.
    """
    if isinstance(content, str):
        content = content.encode(encoding)

    checksum = sha256()
    checksum.update(content)

    return checksum.hexdigest()


def file_checksum(path: Path, CHUNK_SIZE: int = 4096) -> str:
    """Returns the checksum of the file found at the given path.
    The checksum is done using the sha256 algorithm.

    Args:
        path (Path):
            The location where the file is located.
        CHUNK_SIZE (int, optional):
            The size of each chunk of file that will be read.
            Defaults to 4096.

    Returns:
        str:
            The checksum value for the given file.
    """
    checksum = sha256()

    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            checksum.update(chunk)

    return checksum.hexdigest()
