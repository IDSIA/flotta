from .generate import SymmetricKey

from typing import Iterable

from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from base64 import b64decode

import json
import logging

LOGGER = logging.getLogger(__name__)


def decrypt(private_key: RSAPrivateKey, text: str, encoding: str = 'utf8') -> str:
    """Decrypt a text using the given private key.

    :param private_key:
        Private Key to use.
    :param text:
        Content to be decrypted.
    """

    b64_text: bytes = text.encode(encoding)
    enc_text: bytes = b64decode(b64_text)
    plain_text: bytes = private_key.decrypt(enc_text, padding.PKCS1v15())
    ret_text: str = plain_text.decode(encoding)
    return ret_text


def decode_stream(chunks: Iterable, private_key: RSAPrivateKey, SEPARATOR: bytes = b'\n', encoding: str = 'utf8') -> bytes:
    first_part = []
    preamble = True
    decryptor = None

    for chunk in chunks:
        if chunk == SEPARATOR:
            # recover key
            preamble = False

            preamble_bytes: bytes = b''.join(first_part)
            decoded: dict[str, str] = json.loads(decrypt(private_key, preamble_bytes))

            symmetric_key = SymmetricKey(
                decoded['key'].encode(encoding),
                decoded['iv'].encode(encoding),
            )

            decryptor = symmetric_key.decryptor()

        elif preamble:
            first_part.append(chunk)

        else:
            assert decryptor is not None
            yield decryptor.update(chunk)

    yield decryptor.finalize()
