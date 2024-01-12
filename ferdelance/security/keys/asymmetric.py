from __future__ import annotations

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.padding import PSS, MGF1
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey, generate_private_key
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_ssh_public_key


class PrivateKey:
    def __init__(self, data: str | bytes | None = None, encoding: str = "utf8") -> None:
        """Creates a new private key. If the data parameters is not set, then
        generates a new key of size 4096.

        Args:
            data (str | bytes | None, optional):
                Input data collected from somewhere. If it is of type str, then
                the data will be converted to bytes using the encoding parameter.
                Defaults to None.
            encoding (str, optional):
                Encoding to use to convert data to bytes.
                Defaults to "utf8".
        """
        if data is None:
            self.key: RSAPrivateKey = generate_private_key(
                public_exponent=65537,
                key_size=4096,
                backend=default_backend(),
            )

        else:
            if isinstance(data, str):
                data = data.encode(encoding)

            self.key: RSAPrivateKey = load_pem_private_key(data, password=None, backend=default_backend())  # type: ignore

    def bytes(self) -> bytes:
        """Get the private bytes from the private key.

        This is useful when writing the key to a binary sink, like a database
        or a file.

        :return:
            The bytes representation of this private key.
        """
        return self.key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def public_key(self) -> PublicKey:
        return PublicKey(self.key.public_key())

    def decrypt(self, data: str | bytes, encoding: str = "utf8") -> bytes:
        """Decrypt a text using a private key.

        :param text:
            Content to be decrypted.
        :param encoding:
            Encoding to use in the string-byte conversion.
        :return:
            The decoded string.
        """
        if isinstance(data, str):
            data = data.encode(encoding)

        return self.key.decrypt(data, padding.PKCS1v15())

        # b64_text: bytes = text.encode(encoding)
        # enc_text: bytes = b64decode(b64_text)
        # plain_text: bytes = self.key.decrypt(enc_text, padding.PKCS1v15())
        # ret_text: str = plain_text.decode(encoding)
        # return ret_text

    def sign(self, data: str | bytes, encoding: str = "utf8"):
        """Generates a signature string for the given data using the given
        private key.

        Args:
            private_key (RSAPrivateKey):
                Private key to use.
            data (str | bytes):
                Message to sign.
            encoding (str, optional):
                Encoding to use.
                Defaults to "utf8".

        Returns:
            str:
                The signature that can be verified by using the relative
                public key.
        """
        if isinstance(data, str):
            data = data.encode(encoding)

        return self.key.sign(
            data,
            PSS(mgf=MGF1(SHA256()), salt_length=PSS.MAX_LENGTH),
            SHA256(),
        )


class PublicKey:
    def __init__(self, data: bytes | str | RSAPublicKey, encoding: str = "utf8") -> None:
        if isinstance(data, RSAPublicKey):
            self.key: RSAPublicKey = data
        else:
            if isinstance(data, str):
                data = data.encode(encoding)

            self.key: RSAPublicKey = load_ssh_public_key(data)  # type: ignore

    def bytes(self) -> bytes:
        return self.key.public_bytes(
            encoding=serialization.Encoding.OpenSSH,
            format=serialization.PublicFormat.OpenSSH,
        )

    def encrypt(self, data: str | bytes, encoding: str = "utf8") -> bytes:
        """Generates a signature string for the given data.

        Args:
            data (str | bytes):
                Content to be encrypted. If the content is of str type,
                then it will be converted to bytes using the encoding
                arguments.

            encoding (str, optional):
                Encoding to use in the string-byte conversion.
                Defaults to "utf8".

        Returns:
            str:
                The encrypted data.
        """

        if isinstance(data, str):
            data = data.encode(encoding)
        enc_text: bytes = self.key.encrypt(data, padding.PKCS1v15())

        return enc_text

        # b64_text: bytes = b64encode(enc_text)
        # ret_text: str = b64_text.decode(encoding)
        # return ret_text

    def verify(self, data: str | bytes, signature: str | bytes, encoding: str = "utf8") -> None:
        """Verifies that the given data has been signed with the private part of
        the this public key, meaning that the signature is valid.

        Args:
            data (str | bytes):
                Received signed data to verify. If it is of str type, then
                it will be converted to bytes using the encoding argument.
            signature (str | bytes):
                Received signature to compare with. If it is of str type, then
                it will be converted to bytes using the encoding argument.

            encoding (str, optional):
                Encoding to use for arguments of str type.
                Defaults to "utf8".

            Raises:
                InvalidSignature if the signature is not valid.
        """

        if isinstance(data, str):
            data = data.encode(encoding)

        if isinstance(signature, str):
            signature = signature.encode(encoding)

        self.key.verify(
            signature,
            data,
            PSS(mgf=MGF1(SHA256()), salt_length=PSS.MAX_LENGTH),
            SHA256(),
        )
