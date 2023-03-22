from collections.abc import Iterator

from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from .generate import SymmetricKey
from .commons import DEFAULT_SEPARATOR

from base64 import b64decode
from hashlib import sha256

import json
import logging

LOGGER = logging.getLogger(__name__)


def decode_from_transfer(text: str, encoding: str = "utf8") -> str:
    """Decode the string received through a transfer between client and server.

    :param text:
        Text to decode.
    :param encoding:
        Encoding to use in the string-byte conversion.
    :return:
        Decoded bytes.
    """
    in_bytes: bytes = text.encode(encoding)
    b64_bytes: bytes = b64decode(in_bytes)
    out_text: str = b64_bytes.decode(encoding)
    return out_text


def decrypt(private_key: RSAPrivateKey, text: str, encoding: str = "utf8") -> str:
    """Decrypt a text using a private key.

    :param private_jey:
        Local private key.
    :param text:
        Content to be decrypted.
    :param encoding:
        Encoding to use in the string-byte conversion.
    :return:
        The decoded string.
    """
    b64_text: bytes = text.encode(encoding)
    enc_text: bytes = b64decode(b64_text)
    plain_text: bytes = private_key.decrypt(enc_text, padding.PKCS1v15())
    ret_text: str = plain_text.decode(encoding)
    return ret_text


def decrypt_hybrid_key(preamble: bytes, private_key: RSAPrivateKey, encoding="utf8") -> SymmetricKey:
    """Decodes the preamble of a stream, containing the symmetric key for hybrid encryption.

    :param preamble:
        Content read from the stream.
    :param private_key:
        Local private key.
    :param encoding:
        Encoding to use in the string-byte conversion.
    :return:
        The SymmetricKey to use.
    """

    preamble_bytes: bytes = private_key.decrypt(preamble, padding.PKCS1v15())
    preamble_str: str = preamble_bytes.decode("utf8")

    decoded: dict = json.loads(preamble_str)

    return SymmetricKey(
        b64decode(decoded["key"].encode(encoding)),
        b64decode(decoded["iv"].encode(encoding)),
    )


class HybridDecrypter:
    """Decryptor object that uses an hybrid-encryption algorithm.

    The input data except the format of the HybridEncrypter class: it must contain
    a first part with the encrypted symmetric key and a second part with the real
    encrypted data to decrypt. The Separator bytes used in both algorithms need to
    be the same.

    This class will decrypt the first part, obtain the symmetric key and start
    decrypting the data.
    """

    def __init__(
        self, private_key: RSAPrivateKey, SEPARATOR: bytes = DEFAULT_SEPARATOR, encoding: str = "utf8"
    ) -> None:
        """
        :param private_key:
            Server private key.
        :param SEPARATOR:
            Single or sequence of bytes that separates the first part of the stream
            from the second part.
        :param encoding:
            Encoding to use in the string-byte conversion.
        """
        self.private_key: RSAPrivateKey = private_key
        self.SEPARATOR: bytes = SEPARATOR
        self.encoding: str = encoding

        self.decryptor = None
        self.checksum = None

        self.data: bytearray = bytearray()
        self.preamble_found: bool = False

    def decrypt(self, content: bytes) -> str:
        """Decrypt the whole content.

        :param content:
            Data to decrypt.
        :return:
           Decrypted input data in string format.
        """
        data: bytearray = bytearray()

        data.extend(self.start())
        data.extend(self.update(content))
        data.extend(self.end())

        return data.decode(self.encoding)

    def decrypt_stream_to_file(self, stream_func: Iterator[bytes], out_path: str) -> None:
        """Consumer method takes an iterable of chunks produced by an HybridEncryptor object.

        Decrypted data are save to disk at the given location.

        :param out_path:
            Location on disk where to save the decrypted content.
        :param stream_func:
            Iterable of bytes chunks (could be a list or an iterator or a stream) generated by
            the `encrypt_file_to_stream()` or `encrypt_to_stream()` method, to be decoded.
        """
        with open(out_path, "wb") as f:
            data = self.start()
            f.write(data)

            for chunk in stream_func:
                data = self.update(chunk)
                f.write(data)

            data = self.end()
            f.write(data)

    def decrypt_stream(self, stream_func: Iterator[bytes]) -> str:
        """Consumer method that takes an iterable of chunks produced by an HybridEncryptor object.

        Decrypted data are joined in a single output stored in memory.

        :param stream_func:
            Iterable of bytes chunks (could be a list or an iterator or a stream) generated by
            the `encrypt_file_to_stream()` or `encrypt_to_stream()` method, to be decoded.
        :return:
            Decrypted content received.
        """
        data: bytearray = bytearray()

        data.extend(self.start())

        for chunk in stream_func:
            data.extend(self.update(chunk))

        data.extend(self.end())

        return data.decode(self.encoding)

    def start(self) -> bytes:
        """Initialize the decryption algorithm.

        Each time this function is called, the inner status of the object is reset.

        :return:
            Encrypted bytes.
        """
        self.data = bytearray()
        self.checksum = sha256()
        return b""

    def update(self, content: bytes) -> bytes:
        """Decrypt the given content.

        This method requires that the `start()` method is called first.

        This method can be executed multiple times. When all the content has been
        decrypted, remember to call the `end()` method to decrypt the last bytes.

        :param content:
            Data to decrypt.
        :return:
            Decrypted bytes.
        """
        if self.checksum is None or self.data is None:
            raise ValueError("Call the start() method before update(...)")

        if not self.preamble_found:
            self.data.extend(content)

            pos = self.data.find(self.SEPARATOR)

            if pos > 0:
                self.preamble_found = True

                data = bytes(self.data)

                preamble_bytes: bytes = data[:pos]
                symmetric_key: SymmetricKey = decrypt_hybrid_key(preamble_bytes, self.private_key, self.encoding)

                self.decryptor = symmetric_key.decryptor()

                data: bytes = self.decryptor.update(data[pos + len(self.SEPARATOR) :])
                self.checksum.update(data)
                return data

            return b""

        else:
            if self.checksum is None or self.decryptor is None:
                raise ValueError("Cannot decrypt, data may be corrupted")

            data: bytes = self.decryptor.update(content)
            self.checksum.update(data)
            return data

    def end(self) -> bytes:
        """Finalize and end the decryption process.

        Remember to include the output of this method the the decrypted output.

        :return:
            Decrypted string.
        """
        if self.decryptor is None or self.checksum is None:
            raise ValueError("Call the start() method before end()")

        data = self.decryptor.finalize()
        self.checksum.update(data)
        return data

    def get_checksum(self) -> str:
        """Checksum of the transferred data that can be used to check if the data has
        been transferred correctly.
        """
        if self.checksum is None:
            raise ValueError("No decryption performed")

        return self.checksum.hexdigest()
