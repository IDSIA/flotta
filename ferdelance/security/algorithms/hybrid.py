from ferdelance.security.algorithms.core import EncryptionAlgorithm, DecryptionAlgorithm
from ferdelance.security.commons import DEFAULT_SEPARATOR
from ferdelance.security.keys import SymmetricKey, PrivateKey, PublicKey

from hashlib import sha256


class HybridEncryptionAlgorithm(EncryptionAlgorithm):

    """Encryption object that uses an hybrid-encryption algorithm.

    The output data is composed by two parts: the first part contains the symmetric
    key encrypted with the client asymmetric key; the second part contains the
    content encrypted using the asymmetric key.

    The client is expected to decrypt the first part, obtain the symmetric key and
    start decrypting the data.
    """

    def __init__(self, public_key: PublicKey, SEPARATOR: bytes = DEFAULT_SEPARATOR, encoding: str = "utf8") -> None:
        """
        :param public_key:
            Component public key.
        :param SEPARATOR:
            Single or sequence of bytes that separates the first part of the stream
            from the second part.
        :param encoding:
            Encoding to use in the string-byte conversion.
        """
        self.public_key: PublicKey = public_key
        self.SEPARATOR: bytes = SEPARATOR
        self.encoding: str = encoding

        self.symmetric_key = None
        self.encryptor = None
        self.checksum = None

    def start(self) -> bytes:
        """Initialize the encryption algorithm.

        Each time this function is called, the inner status of the object is reset.

        :return:
            Encrypted bytes.
        """
        self.symmetric_key = SymmetricKey()
        preamble = self.public_key.encrypt(self.symmetric_key.bytes(), self.encoding)

        self.encryptor = self.symmetric_key.encryptor()
        self.checksum = sha256()

        content = bytearray()

        content += preamble
        content += self.SEPARATOR

        return bytes(content)

    def update(self, content: str | bytes) -> bytes:
        """Encrypt the given content.

        This method requires that the `start()` method is called first.

        This method can be executed multiple times. When all the content has been
        encrypted, remember to call the `end()` method.

        :param content:
            Data to encrypt.
        :return:
            Encrypted bytes.
        """
        if self.checksum is None or self.encryptor is None:
            raise ValueError("Call the start() method before update(...)")

        if isinstance(content, str):
            content = content.encode(self.encoding)

        self.checksum.update(content)
        return self.encryptor.update(content)

    def end(self) -> bytes:
        """Finalize and end the encryption process.

        Remember to include the output of this method the the encrypter output.

        :return:
            Encrypted bytes.
        """
        if self.encryptor is None:
            raise ValueError("Call the start() method before end()")

        return self.encryptor.finalize()

    def get_checksum(self) -> str:
        """Checksum of the original data that can be used to check if the data has
        been transferred correctly.
        """
        if self.checksum is None:
            raise ValueError("No encryption performed")

        return self.checksum.hexdigest()


class HybridDecryptionAlgorithm(DecryptionAlgorithm):
    """Decryptor object that uses an hybrid-encryption algorithm.

    The input data except the format of the HybridEncrypter class: it must contain
    a first part with the encrypted symmetric key and a second part with the real
    encrypted data to decrypt. The Separator bytes used in both algorithms need to
    be the same.

    This class will decrypt the first part, obtain the symmetric key and start
    decrypting the data.
    """

    def __init__(self, private_key: PrivateKey, SEPARATOR: bytes = DEFAULT_SEPARATOR, encoding: str = "utf8") -> None:
        """
        :param private_key:
            Server private key.
        :param SEPARATOR:
            Single or sequence of bytes that separates the first part of the stream
            from the second part.
        :param encoding:
            Encoding to use in the string-byte conversion.
        """
        self.private_key: PrivateKey = private_key
        self.SEPARATOR: bytes = SEPARATOR
        self.encoding: str = encoding

        self.decryptor = None
        self.checksum = None

        self.data: bytearray = bytearray()
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

                enc_preamble: bytes = data[:pos]
                preamble = self.private_key.decrypt(enc_preamble)
                symmetric_key: SymmetricKey = SymmetricKey(data=preamble)

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
