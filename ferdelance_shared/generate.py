from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey, generate_private_key
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_ssh_public_key

import os


def generate_private_key() -> RSAPrivateKey:
    """Generates a new private key."""
    private_key: RSAPrivateKey = generate_private_key(
        public_exponent=65537,
        key_size=4096,
        backend=default_backend(),
    )

    return private_key


def private_key_from_bytes(data: bytes) -> RSAPrivateKey:
    """Convert the input data in a valid private key.

    This is useful when reading the key from a binary source, like a database 
    or a filesystem.

    :param data:
        Input data collected from somewhere.
    :return:
        A valid private key.
    """
    return load_pem_private_key(data, backend=default_backend())


def private_key_from_str(data: str, encoding: str = 'utf8') -> RSAPrivateKey:
    """Convert a str to a valid private key.

    This is useful when reading the key from an encoded source, like a database.

    :param data:
        Input data collected from somewhere.
    :param encoding:
        Encoding to use for the conversion to bytes.
    :return:
        A valid private key.
    """
    return private_key_from_bytes(data.encode(encoding))


def bytes_from_private_key(private_key: RSAPrivateKey) -> bytes:
    """Get the private bytes from a private key.

    This is useful when writing the key to a binary sink, like a database 
    or a filesystem.

    :param private_key:
        Input private key to convert to bytes.
    :return:
        A valid private key in bytes format
    """
    private_bytes: bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return private_bytes


def public_key_from_bytes(data: bytes) -> RSAPublicKey:
    """Convert the input data in a valid public key.

    This is useful when reading the key from a binary source, like a filesystem.

    :param data:
        Input data collected from somewhere.
    :return:
        A valid public key.
    """
    return load_ssh_public_key(data, backend=default_backend())


def public_key_from_str(data: str, encoding: str = 'utf8') -> RSAPrivateKey:
    """Convert a str to a valid public key.

    This is useful when reading the key from an encoded source, like a database.

    :param data:
        Input data collected from somewhere.
    :param encoding:
        Encoding to use for the conversion to bytes.
    :return:
        A valid public key.
    """
    return public_key_from_bytes(data.encode(encoding))


def bytes_from_public_key(public_key: RSAPublicKey) -> bytes:
    """Get the public bytes from a public key.

    This is useful when writing the key to a binary sink, like a database 
    or a filesystem.

    :param public_key:
        Input public key to convert to bytes.
    :return:
        A valid public key in bytes format
    """
    public_bytes: bytes = public_key.public_bytes(
        encoding=serialization.Encoding.OpenSSH,
        format=serialization.PublicFormat.OpenSSH,
    )

    return public_bytes


class SymmetricKey:
    def __init__(self, key: bytes | None, iv: bytes | None, key_size: int = 32, iv_size: int = 16) -> None:
        """Generates a new random key, initialization vector, and cipher for 
        a symmetric encryption algorithm.

        :param key:
            If set, uses these bytes as key, otherwise generates a new key.
        :param iv:
            If set, uses these bytes as initialization vector, otherwise generates a new vector.
        :param key_size:
            Size of the key to generate.
        :param iv_size:
            Size of the initialization vector.
        """

        self.key: bytes = key if key else os.urandom(key_size)
        self.iv: bytes = iv if iv else os.urandom(iv_size)

        self.cipher: Cipher = Cipher(
            algorithms.AES(self.key),
            modes.CTR(self.iv),
            backend=default_backend()
        )

    def encryptor(self):
        """Get a encryptor from the cipher."""
        return self.cipher.encryptor()

    def decryptor(self):
        """Get a decryptor from the cipher."""
        return self.cipher.decryptor()
