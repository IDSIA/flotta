from collections.abc import Iterator

from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from .generate import SymmetricKey
from .commons import DEFAULT_SEPARATOR

from base64 import b64encode
from hashlib import sha256

import json
import logging

LOGGER = logging.getLogger(__name__)


def encode_to_transfer(text: str, encoding: str = 'utf8') -> str:
    """Encode a string that will be sent through a transfer between client and server.

    :param text:
        Text to decode.
    :param encoding:
        Encoding to use in the string-byte conversion.
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
    :param encoding:
        Encoding to use in the string-byte conversion.
    """
    plain_text: bytes = text.encode(encoding)
    enc_text: bytes = public_key.encrypt(plain_text, padding.PKCS1v15())
    b64_text: bytes = b64encode(enc_text)
    ret_text: str = b64_text.decode(encoding)
    return ret_text


def generate_hybrid_encryption_key(public_key: RSAPublicKey, encoding: str = 'utf8') -> tuple[bytes, SymmetricKey]:
    """Generates a one-use Symmetric key for hybrid encryption and returns it both
    in bytes and in object form.

    :param private_key:
        Remote public key.
    :param encoding:
        Encoding to use in the string-byte conversion.
    :return:
        A tuple containing the preamble in bytes to sent to the remote receiver,
        the SymmetricKey to use to encrypt the remaining of the stream.
    """

    # generate session key for hybrid encryption
    symmetric_key = SymmetricKey()

    preamble_str: str = json.dumps({
        'key': b64encode(symmetric_key.key).decode(encoding),
        'iv': b64encode(symmetric_key.iv).decode(encoding),
    })

    # first part: return encrypted session key
    preamble_bytes: bytes = preamble_str.encode(encoding)
    preamble: bytes = public_key.encrypt(preamble_bytes, padding.PKCS1v15())

    return preamble, symmetric_key


class HybridEncrypter:
    """Encryption object that uses an hybrid-encryption algorithm.

    The output data is composed by two parts: the first part contains the symmetric
    key encrypted with the client asymmetric key; the second part contains the 
    content encrypted using the asymmetric key.

    The client is expected to decrypt the first part, obtain the symmetric key and 
    start decrypting the data.
    """

    def __init__(self, public_key: RSAPublicKey, SEPARATOR: bytes = DEFAULT_SEPARATOR, encoding: str = 'utf8') -> None:
        """
        :param public_key:
            Client public key.
        :param SEPARATOR:
            Single or sequence of bytes that separates the first part of the stream
            from the second part.
        :param encoding:
            Encoding to use in the string-byte conversion.
        """
        self.public_key: RSAPublicKey = public_key
        self.SEPARATOR: bytes = SEPARATOR
        self.encoding: str = encoding

        self.preamble, self.symmetric_key = generate_hybrid_encryption_key(public_key, encoding)

        self.encryptor = None
        self.checksum = None

    def encrypt(self, content: str) -> bytes:
        """Encrypt the whole content.

        :param content:
            Data to encrypt.
        :return:
            Encrypted input data in byte format.
        """
        enc_content: bytearray = bytearray()

        enc_content += self.start()
        enc_content += self.update(content)
        enc_content += self.end()

        return bytes(enc_content)

    def encrypt_file_to_stream(self, in_path: str, CHUNK_SIZE: int = 4096) -> Iterator[bytes]:
        """Generator function that encrypt an input file read from disk.

        :param in_path:
            Path on disk of the file to stream.
        :param CHUNK_SIZE:
            Size in bytes of each chunk transmitted to the client.
        :return:

        """
        yield self.start()

        with open(in_path, 'r') as f:
            while chunk := f.read(CHUNK_SIZE):
                yield self.update(chunk)

        yield self.end()

    def encrypt_to_stream(self, content: str, CHUNK_SIZE: int = 4096) -> Iterator[bytes]:
        """Generator function that streams the given content.

        :param content:
            Content to stream in string format.
        :param CHUNK_SIZE:
            Size in bytes of each chunk transmitted to the client.
        :return:
            A stream of bytes
        """
        yield self.start()

        n = len(content)

        start: int = 0
        end: int = CHUNK_SIZE

        while True:
            chunk = content[start:end]

            if len(chunk) == 0:
                break

            yield self.update(chunk)

            start = end
            end = min(end + CHUNK_SIZE, n)

        yield self.end()

    def start(self) -> bytes:
        """Initialize the encryption algorithm.

        Each time this function is called, the inner status of the object is reset.

        :return:
            Encrypted bytes.
        """
        self.encryptor = self.symmetric_key.encryptor()
        self.checksum = sha256()

        content = bytearray()

        content += self.preamble
        content += self.SEPARATOR

        return bytes(content)

    def update(self, content: str) -> bytes:
        """Encrypt the given content.

        This method requires that the `start()` method is called first.

        This method can be executed multiple times. When all the content has been
        encrypted, remember to call the `end()` method.

        :param content:
            Data to encrypt.
        :return:
            Encrypted bytes.
        """
        data = content.encode(self.encoding)
        self.checksum.update(data)
        return self.encryptor.update(data)

    def end(self) -> bytes:
        """Finalize and end the encryption process.

        Remember to include the output of this method the the encrypter output.

        :return:
            Encrypted bytes.
        """
        return self.encryptor.finalize()

    def get_checksum(self) -> str:
        """Checksum of the original data that can be used to check if the data has 
        been transferred correctly.
        """
        return self.checksum.hexdigest()
