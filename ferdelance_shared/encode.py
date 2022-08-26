from .generate import SymmetricKey

from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from base64 import b64encode

import json
import logging

LOGGER = logging.getLogger(__name__)


def encode_to_transfer(text: str, encoding: str = 'utf8') -> str:
    """Encode a string that will be sent through a transfer between client and server.

    :param text:
        Text to decode.
    :param encoding:
        Encoding to use.
    :return:
        Encoded text.
    """
    in_bytes: bytes = text.encode(encoding)
    b64_bytes: bytes = b64encode(in_bytes)
    out_text: str = b64_bytes.decode(encoding)
    return out_text


def encrypt(public_key: RSAPublicKey, text: str, encoding: str = 'utf8') -> str:
    """Encrypt a text using a public key.

    :param public_key:
        Target public key.
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

    LOGGER.debug(f'preamble sent {data_str}')

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
