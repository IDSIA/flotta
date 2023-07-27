from typing import Any, Iterator
from .generate import (
    generate_asymmetric_key,
    bytes_from_private_key,
    bytes_from_public_key,
    private_key_from_bytes,
    public_key_from_bytes,
    public_key_from_str,
    RSAPrivateKey,
    RSAPublicKey,
)
from .decode import (
    HybridDecrypter,
    decode_from_transfer,
    decrypt,
)
from .encode import (
    HybridEncrypter,
    encode_to_transfer,
    encrypt,
)

from requests import Response

import os
import json


class Exchange:
    def __init__(self) -> None:
        self.private_key: RSAPrivateKey | None = None
        self.public_key: RSAPublicKey | None = None
        self.remote_key: RSAPublicKey | None = None

        self.token: str | None = None

    def generate_key(self) -> None:
        """Generates a new pair of asymmetric keys."""
        self.private_key = generate_asymmetric_key()
        self.public_key = self.private_key.public_key()

    def load_key(self, path: str) -> None:
        """Load a private key from disk.

        :param path:
            Location of the private key on disk to load from.
        :raise:
            ValueError if the path does not exists.
        """
        if not os.path.exists(path):
            raise ValueError(f"SSH key file {path} does not exists")

        with open(path, "rb") as f:
            pk_bytes: bytes = f.read()
            self.private_key = private_key_from_bytes(pk_bytes)
            self.public_key = self.private_key.public_key()

    def load_remote_key(self, path: str) -> None:
        """Load a remote public key from disk.

        :param path:
            Location of the remote key on disk to load from.
        :raise:
            ValueError if the path does not exists.
        """
        if not os.path.exists(path):
            raise ValueError(f"SSH key file {path} does not exists")

        with open(path, "rb") as f:
            rk_bytes: bytes = f.read()
            self.remote_key = public_key_from_bytes(rk_bytes)

    def save_private_key(self, path: str) -> None:
        """Save the stored private key to disk.

        :param path:
            Location of the private key to save to.
        :raise:
            ValueError if the path already exists or the private key
            is not available.
        """
        if os.path.exists(path):
            raise ValueError(f"destination path {path} already exists")

        if self.private_key is None:
            raise ValueError("cannot save: no private key available")

        with open(path, "wb") as f:
            pk_bytes: bytes = bytes_from_private_key(self.private_key)
            f.write(pk_bytes)

    def save_public_key(self, path: str) -> None:
        """Save the stored public key to disk.

        :param path:
            Location of the private key to save to.
        :raise:
            ValueError if the path already exists or the public key
            is not available.
        """
        if os.path.exists(path):
            raise ValueError(f"destination path {path} already exists")

        if self.public_key is None:
            raise ValueError("cannot save: no public key available")

        with open(path, "wb") as f:
            pk_bytes: bytes = bytes_from_public_key(self.public_key)
            f.write(pk_bytes)

    def save_remote_key(self, path: str) -> None:
        """Save the stored remote public key to disk.

        :param path:
            Location of the private key to save to.
        :raise:
            ValueError if the path already exists or the remote key
            is not available.
        """
        if os.path.exists(path):
            raise ValueError(f"destination path {path} already exists")

        if self.remote_key is None:
            raise ValueError("cannot save: no public key available")

        with open(path, "wb") as f:
            pk_bytes: bytes = bytes_from_public_key(self.remote_key)
            f.write(pk_bytes)

    def set_key_bytes(self, private_key_bytes: bytes) -> None:
        """Set the private key and the public key from a private key
        in bytes format.

        :param private_key_bytes:
            Private key stored in memory and in bytes format.
        """
        self.private_key = private_key_from_bytes(private_key_bytes)
        self.public_key = self.private_key.public_key()

    def get_private_key_bytes(self) -> bytes:
        """Get the private key in bytes format to be stored somewhere.

        :raise:
            ValueError if there is no private key.
        """
        if self.private_key is None:
            raise ValueError("No private key available")

        return bytes_from_private_key(self.private_key)

    def get_public_key_bytes(self) -> bytes:
        """Get the public key in bytes format to be stored somewhere.

        :raise:
            ValueError if there is no public key.
        """
        if self.public_key is None:
            raise ValueError("No public key available")

        return bytes_from_public_key(self.public_key)

    def set_token(self, token: str) -> None:
        """Set the token for authentication.

        :param token:
            Token to use.
        """
        self.token = token

    def set_remote_key(self, data: str, encoding: str = "utf8") -> None:
        """Decode and set a public key from a remote host.

        :param data:
            String content not yet decoded.
        :param encoding:
            Encoding to use in the string-byte conversion.
        """
        self.remote_key = public_key_from_str(decode_from_transfer(data, encoding), encoding)

    def set_remote_key_bytes(self, remote_key: bytes) -> None:
        """Set the public key of the remote host from bytes stored in memory.

        :param remote_key:
            Public key of a remote host stored in memory and in bytes format.
        """
        self.remote_key = public_key_from_bytes(remote_key)

    def transfer_public_key(self, encoding: str = "utf8") -> str:
        """Encode the stored public key for secure transfer to remote host.

        :return:
            An encoded public key in string format.
        :param encoding:
            Encoding to use in the string-byte conversion.
        :raise:
            ValueError if there is no public key available.
        """
        if self.public_key is None:
            raise ValueError("public key not set")

        data: str = bytes_from_public_key(self.public_key).decode(encoding)
        return encode_to_transfer(data, encoding)

    def encrypt(self, content: str, encoding: str = "utf8") -> str:
        """Encrypt a text for the remote host using the stored remote public key.

        :param content:
            Content to be encrypted.
        :param encoding:
            Encoding to use in the string-byte conversion.
        :raise:
            ValueError if there is no remote public key.
        """
        if self.remote_key is None:
            raise ValueError("No remote host public key available")

        return encrypt(self.remote_key, content, encoding)

    def decrypt(self, content: str, encoding: str = "utf8") -> str:
        """Decrypt a text encrypted by the remote host with the public key,
        using the stored private key.

        :param content:
            Content to be decrypted.
        :param encoding:
            Encoding to use in the string-byte conversion.
        :raise:
            ValueError if there is no private key.
        """
        if self.private_key is None:
            raise ValueError("No private key available")

        return decrypt(self.private_key, content, encoding)

    def headers(self) -> dict[str, str]:
        """Build headers for authentication.

        :return:
            The headers to use for authentication.
        :raise:
            ValueError if not token is available.
        """
        if self.token is None:
            raise ValueError("token not set")

        return {"Authorization": f"Bearer {self.token}"}

    def create_payload(self, content: dict[str, Any], encoding: str = "utf8") -> bytes:
        """Convert a dictionary in a JSON object in string format, then
        encode it for transfer using an hybrid encryption algorithm.

        :param content:
            The dictionary to encrypt.
        :param encoding:
            Encoding to use in the string-byte conversion.
        :raise:
            ValueError if the remote key is not set.
        """
        if self.remote_key is None:
            raise ValueError("No public remote key available")

        payload: str = json.dumps(content, default=str)

        return HybridEncrypter(self.remote_key, encoding=encoding).encrypt(payload)

    def get_payload(self, content: bytes, encoding: str = "utf8") -> dict[str, Any]:
        """Convert the received content in bytes format to a dictionary
        assuming the content is a JSON object in string format, then
        decode it using an hybrid encryption algorithm.

        :param content:
            The content to decrypt.
        :param encoding:
            Encoding to use in the string-byte conversion.
        :return:
            A JSON object in dictionary format.
        :raise:
            ValueError if the private key is not set.
        """
        if self.private_key is None:
            raise ValueError("No private key available")

        return json.loads(HybridDecrypter(self.private_key, encoding=encoding).decrypt(content))

    def stream(self, content: str | bytes, encoding: str = "utf8") -> Iterator[bytes]:
        """Creates a stream from content in memory.

        :param content:
            The content to stream to the remote host.
        :param encoding:
            Encoding to use in the string-byte conversion.
        :return:
            An iterator that can be consumed to produce the stream.
        :raise:
            ValueError if the remote host key is not set.
        """
        if self.remote_key is None:
            raise ValueError("No remote key available")

        enc = HybridEncrypter(self.remote_key, encoding=encoding)

        return enc.encrypt_to_stream(content)

    def stream_from_file(self, path: str, encoding: str = "utf8") -> Iterator[bytes]:
        """Creates a stream from content from a file.

        :param path:
            The path where the content to stream is located.
        :param encoding:
            Encoding to use in the string-byte conversion.
        :return:
            An iterator that can be consumed to produce the stream.
        :raise:
            ValueError if the remote host key is not set.
        """
        if self.remote_key is None:
            raise ValueError("No remote key available")

        enc = HybridEncrypter(self.remote_key, encoding=encoding)

        return enc.encrypt_file_to_stream(path)

    def stream_response(self, content: Iterator[bytes], encoding: str = "utf8") -> tuple[bytes, str]:
        """Consumes the stream content of a response, and save the content in memory.

        :param stream:
            A requests.Response opened with the attribute `stream=True`.
        :param encoding:
            Encoding to use in the string-byte conversion.
        :raise:
            ValueError if no private key is available.
        """
        if self.private_key is None:
            raise ValueError("No private key available")

        dec = HybridDecrypter(self.private_key, encoding=encoding)

        data = dec.decrypt_stream(content)
        return data, dec.get_checksum()

    def stream_response_to_file(
        self, stream: Response, path: str, encoding: str = "utf8", CHUNK_SIZE: int = 4096
    ) -> str:
        """Consumes the stream content of a response, and save the content to file.

        :param stream:
            A requests.Response opened with the attribute `stream=True`.
        :param path:
            Location on disk to save the download content to.
        :param encoding:
            Encoding to use in the string-byte conversion.
        :raise:
            ValueError if no private key is available or the path already exists.
        """
        if self.private_key is None:
            raise ValueError("No private key available")

        if os.path.exists(path):
            raise ValueError(f"path {path} already exists")

        dec = HybridDecrypter(self.private_key, encoding=encoding)
        dec.decrypt_stream_to_file(stream.iter_content(chunk_size=CHUNK_SIZE), path)

        return dec.get_checksum()

    def encrypt_file_for_remote(self, path_in: str, path_out: str) -> None:
        """Encrypt a file from disk to another file on disk. This file can be sent
        to the remote host.

        :param path_in:
            Source file to encrypt.
        :param path_out:
            Destination path of the encrypted file.
        :raise:
            ValueError if there is no remote key available.
        """

        if self.remote_key is None:
            raise ValueError("No remote key available")

        enc = HybridEncrypter(self.remote_key)
        with open(path_out, "wb") as w:
            w.write(enc.start())
            with open(path_in, "rb") as r:
                while content := r.read():
                    w.write(enc.update(content))
                w.write(enc.end())

    def encrypt_file(self, path_in: str, path_out: str) -> None:
        """Encrypt a file from disk to another file on disk. This file can be decrypted
        only with a private key.

        :param path_in:
            Source file to encrypt.
        :param path_out:
            Destination path of the decrypted file.
        :raise:
            ValueError if there is no public key available.
        """

        if self.public_key is None:
            raise ValueError("No public key available")

        enc = HybridEncrypter(self.public_key)
        with open(path_out, "wb") as w:
            w.write(enc.start())
            with open(path_in, "rb") as r:
                while content := r.read():
                    w.write(enc.update(content))
                w.write(enc.end())

    def decrypt_file(self, path_in: str, path_out: str) -> None:
        """Decrypt a file from disk to another file on disk. This file can must be
        encrypted with a valid public key.

        :param path_in:
            Source file to decrypt.
        :param path_out:
            Destination path of the decrypted file.
        :raise:
            ValueError if there is no private key available.
        """

        if self.private_key is None:
            raise ValueError("No private key available")

        enc = HybridDecrypter(self.private_key)
        with open(path_out, "wb") as w:
            w.write(enc.start())
            with open(path_in, "rb") as r:
                while content := r.read():
                    w.write(enc.update(content))
                w.write(enc.end())
