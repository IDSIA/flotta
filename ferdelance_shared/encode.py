from .generate import SymmetricKey

from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from base64 import b64encode

import json
import logging

LOGGER = logging.getLogger(__name__)


def encrypt(public_key: RSAPublicKey, text: str, encoding: str = 'utf8') -> str:
    """Encrypt a text to be sent outside of the server.

    :param public_key:
        Client public key in bytes format.
    :param text:
        Content to be encrypted.
    """

    plain_text: bytes = text.encode(encoding)
    enc_text: bytes = public_key.encrypt(plain_text, padding.PKCS1v15())
    b64_text: bytes = b64encode(enc_text)
    ret_text: str = b64_text.decode(encoding)
    return ret_text


def stream_encrypt_file(path: str, public_key: RSAPublicKey, CHUNK_SIZE: int = 4096, SEPARATOR: bytes = b'\n', encoding: str = 'utf8') -> bytes:
    """Generator function that streams a file from the given path and encrpyt
    the content using an hybrid-encryption algorithm.

    The streamed file is composed by two parts: the first part contains the 
    symmetric key encrypted with the client asymmetric key; the second part
    contains the file encrypted using the asymmetric key.

    The client is expected to decrypt the first part, obtain the symmetric key
    and start decrypte the content of the file.

    :param path:
        Path on disk of the file to stream.
    :param public_key:
        Client public key.
    :param CHUNK_SIZE:
        Size in bytes of each chunk transmitted to the client.
    :param SEPARATOR:
        Single or sequence of bytes that separates the first part of the stream
        from the second part.
    :return:
        A stream of bytes
    """

    # generate session key for hiybrid encrpytion
    symmmetric_key = SymmetricKey()

    data_str: str = json.dumps({
        'key': b64encode(symmmetric_key.key).decode(encoding),
        'iv': b64encode(symmmetric_key.iv).decode(encoding),
    })

    # first part: return encrypted session key
    yield encrypt(public_key, data_str)

    # return separator between first and second part
    yield SEPARATOR

    # second part: return encrypted file
    encryptor = symmmetric_key.encryptor()

    with open(path, mode='rb') as f:
        while True:
            chunk = f.read(CHUNK_SIZE)

            if not chunk:
                yield encryptor.finalize()
                break

            yield encryptor.update(chunk)
