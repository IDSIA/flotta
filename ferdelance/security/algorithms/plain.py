from ferdelance.security.algorithms.core import EncryptionAlgorithm, DecryptionAlgorithm
from ferdelance.security.commons import DEFAULT_SEPARATOR
from ferdelance.security.keys import PrivateKey, PublicKey

from hashlib import sha256


class NoEncryptionAlgorithm(EncryptionAlgorithm):
    """This algorithm does not apply any encryption to the data."""

    def __init__(self, _: PublicKey, SEPARATOR: bytes = DEFAULT_SEPARATOR, encoding: str = "utf8") -> None:
        """
        :param SEPARATOR:
            Single or sequence of bytes that separates the first part of the stream
            from the second part.
        :param encoding:
            Encoding to use in the string-byte conversion.
        """
        self.SEPARATOR: bytes = SEPARATOR
        self.encoding: str = encoding

        self.checksum = None

    def start(self) -> bytes:
        self.checksum = sha256()

        content = bytearray()
        content += self.SEPARATOR

        return bytes(content)

    def update(self, content: str | bytes) -> bytes:
        if self.checksum is None:
            raise ValueError("Call the start() method before update(...)")

        if isinstance(content, str):
            content = content.encode(self.encoding)

        self.checksum.update(content)
        return content

    def end(self) -> bytes:
        return b""

    def get_checksum(self) -> str:
        """Checksum of the original data that can be used to check if the data has
        been transferred correctly.
        """
        if self.checksum is None:
            raise ValueError("No encryption performed")

        return self.checksum.hexdigest()


class NoDecryptionAlgorithm(DecryptionAlgorithm):
    """This algorithm does not decrypt any data."""

    def __init__(self, _: PrivateKey, SEPARATOR: bytes = DEFAULT_SEPARATOR, encoding: str = "utf8") -> None:
        """
        :param private_key:
            Server private key.
        :param SEPARATOR:
            Single or sequence of bytes that separates the first part of the stream
            from the second part.
        :param encoding:
            Encoding to use in the string-byte conversion.
        """
        self.SEPARATOR: bytes = SEPARATOR
        self.encoding: str = encoding

        self.checksum = None

        self.data: bytearray
        self.preamble_found: bool = False

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

                data: bytes = data[pos + len(self.SEPARATOR) :]
                self.checksum.update(data)
                return data

            return b""

        else:
            if self.checksum is None:
                raise ValueError("Cannot decrypt, data may be corrupted")

            self.checksum.update(content)
            return content

    def end(self) -> bytes:
        """Finalize and end the decryption process.

        Remember to include the output of this method the the decrypted output.

        :return:
            Decrypted string.
        """
        if self.checksum is None:
            raise ValueError("Call the start() method before end()")

        return b""

    def get_checksum(self) -> str:
        """Checksum of the transferred data that can be used to check if the data has
        been transferred correctly.
        """
        if self.checksum is None:
            raise ValueError("No decryption performed")

        return self.checksum.hexdigest()
