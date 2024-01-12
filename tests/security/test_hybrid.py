from ferdelance.security.algorithms.hybrid import HybridEncryptionAlgorithm, HybridDecryptionAlgorithm
from ferdelance.security.keys.asymmetric import PrivateKey, PublicKey

from tests.utils import random_string

from pathlib import Path

import os


def test_stream_from_memory():
    """Test the encrypting and decrypting of a stream of bytes from a content in memory."""
    private_key: PrivateKey = PrivateKey()
    public_key: PublicKey = private_key.public_key()

    content = random_string(10000)

    enc = HybridEncryptionAlgorithm(public_key)
    dec = HybridDecryptionAlgorithm(private_key)

    chunks_encrypted: list[bytes] = []
    for chunk in enc.encrypt_content_to_stream(content):
        chunks_encrypted.append(chunk)

    dec_content = dec.decrypt_content_stream(iter(chunks_encrypted))

    assert content == dec_content.decode("utf8")


def test_stream_from_file():
    """Test the encrypting and decrypting of a stream of bytes from a file."""
    private_key: PrivateKey = PrivateKey()
    public_key: PublicKey = private_key.public_key()

    enc = HybridEncryptionAlgorithm(public_key)
    dec = HybridDecryptionAlgorithm(private_key)

    content_from: str = random_string(7637)

    path_content_from: Path = Path(".") / "file_in.txt"
    path_content_to: Path = Path(".") / "file_out.txt"

    with open(path_content_from, "w") as f:
        f.write(content_from)

    chunks_encrypted: list[bytes] = []
    for chunk in enc.encrypt_file_to_stream(path_content_from):
        chunks_encrypted.append(chunk)

    dec.decrypt_stream_to_file(iter(chunks_encrypted), path_content_to)

    with open(path_content_to, "rb") as f:
        content_to = f.read().decode("utf8")

    os.remove(path_content_from)
    os.remove(path_content_to)

    assert content_from == content_to


def test_hybrid_encryption():
    private_key: PrivateKey = PrivateKey()
    public_key: PublicKey = private_key.public_key()

    content: str = random_string(1234)

    enc = HybridEncryptionAlgorithm(public_key)
    dec = HybridDecryptionAlgorithm(private_key)

    secret: bytes = enc.encrypt(content)
    message: str = dec.decrypt(secret).decode()

    assert content == message
