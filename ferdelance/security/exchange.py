from typing import Iterator

from ferdelance.security.algorithms.hybrid import HybridDecryptionAlgorithm, HybridEncryptionAlgorithm
from ferdelance.security.checksums import str_checksum, file_checksum
from ferdelance.security.keys.asymmetric import PrivateKey, PublicKey
from ferdelance.security.utils import decode_from_transfer, encode_to_transfer

from base64 import b64decode, b64encode
from pathlib import Path
from requests import Response

import json
import os


class Exchange:
    def __init__(self, private_key_path: Path | None = None, encoding: str = "utf8") -> None:
        self.private_key: PrivateKey | None = None
        self.public_key: PublicKey | None = None
        self.remote_key: PublicKey | None = None

        self.encoding = encoding

        if private_key_path is not None:
            self.load_key(private_key_path)

    def generate_keys(self) -> None:
        """Generates a new pair of asymmetric keys."""
        self.private_key = PrivateKey()
        self.public_key = self.private_key.public_key()

    def load_key(self, path: Path) -> None:
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
            self.private_key = PrivateKey(pk_bytes)
            self.public_key = self.private_key.public_key()

    def load_remote_key(self, path: Path) -> None:
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
            self.remote_key = PublicKey(rk_bytes)

    def store_private_key(self, path: Path) -> None:
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
            pk_bytes: bytes = self.private_key.bytes()
            f.write(pk_bytes)

    def store_public_key(self, path: Path) -> None:
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
            pk_bytes: bytes = self.public_key.bytes()
            f.write(pk_bytes)

    def store_remote_key(self, path: Path) -> None:
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
            pk_bytes: bytes = self.remote_key.bytes()
            f.write(pk_bytes)

    def set_private_key(self, private_key: str | bytes) -> None:
        """Set the private key and the public key from a private key
        in string format and encoded for transfer.

        :param private_key:
            Private key stored in memory and in string encoded for transfer format.
        """
        if isinstance(private_key, str):
            private_key = decode_from_transfer(private_key, self.encoding)

        self.private_key = PrivateKey(private_key)
        self.public_key = self.private_key.public_key()

    def get_private_bytes(self) -> bytes:
        """Get the private key in bytes format to be stored somewhere.

        :raise:
            ValueError if there is no private key.
        """
        if self.private_key is None:
            raise ValueError("No private key available")

        return self.private_key.bytes()

    def get_public_bytes(self) -> bytes:
        """Get the public key in bytes format to be stored somewhere.

        :raise:
            ValueError if there is no public key.
        """
        if self.public_key is None:
            raise ValueError("No public key available")

        return self.public_key.bytes()

    def set_remote_key(self, data: str | bytes) -> None:
        """Decode and set a public key from a remote host.

        :param data:
            String content not yet decoded.
        """
        self.remote_key = PublicKey(data, self.encoding)

    def transfer_public_key(self) -> str:
        """Encode the stored public key for secure transfer to remote host.

        :return:
            An encoded public key in string format.
        :raise:
            ValueError if there is no public key available.
        """
        if self.public_key is None:
            raise ValueError("public key not set")

        return encode_to_transfer(self.public_key.bytes())

    def transfer_private_key(self) -> str:
        """Encode the stored private key for secure transfer as string.

        :return:
            An encoded private key in string format.
        :raise:
            ValueError if there is no private key available.
        """
        if self.private_key is None:
            raise ValueError("public key not set")

        return encode_to_transfer(self.private_key.bytes(), self.encoding)

    def transfer_remote_key(self) -> str:
        """Encode the stored private key for secure transfer as string.

        :return:
            The encoded remote public key in string format.
        :raise:
            ValueError if there is no remote key available.
        """
        if self.remote_key is None:
            raise ValueError("remote public key not set")

        return encode_to_transfer(self.remote_key.bytes(), self.encoding)

    def encrypt(self, content: str) -> bytes:
        """Encrypt a text for the remote host using the stored remote public key.

        :param content:
            Content to be encrypted.
        :raise:
            ValueError if there is no remote public key.
        """
        if self.remote_key is None:
            raise ValueError("No remote host public key available")

        return self.remote_key.encrypt(content, self.encoding)

    def decrypt(self, content: str) -> bytes:
        """Decrypt a text encrypted by the remote host with the public key,
        using the stored private key.

        :param content:
            Content to be decrypted.
        :raise:
            ValueError if there is no private key.
        """
        if self.private_key is None:
            raise ValueError("No private key available")

        return self.private_key.decrypt(content, self.encoding)

    def sign(self, content: str | bytes) -> str:
        if self.private_key is None:
            raise ValueError("Private key not set")

        signature = self.private_key.sign(content, self.encoding)

        return b64encode(signature).decode(self.encoding)

    def verify(self, content: str, signature: str) -> None:
        if self.remote_key is None:
            raise ValueError("Remote key not set")

        self.remote_key.verify(content, b64decode(signature), self.encoding)

    def create_header(self, set_encryption: bool = True) -> dict[str, str]:
        if set_encryption:
            content_encoding = f"encrypted/{self.encoding}"
            accept_encoding = f"encrypted/{self.encoding}"
        else:
            content_encoding = self.encoding
            accept_encoding = self.encoding

        return {
            "Content-Encoding": content_encoding,
            "Accept-Encoding": accept_encoding,
        }

    def create_signed_header(
        self,
        component_id: str,
        checksum: str,
        set_encryption: bool = True,
        extra_headers: dict[str, str] = dict(),
    ) -> dict[str, str]:
        if self.remote_key is None:
            raise ValueError("No public remote key available")

        data_to_sign = f"{component_id}:{checksum}"
        signature = self.sign(data_to_sign)

        token_data = {
            "id": component_id,
            "checksum": checksum,
            "signature": signature,
        } | extra_headers

        enc = HybridEncryptionAlgorithm(self.remote_key, encoding=self.encoding)

        data_json = json.dumps(token_data)
        data_enc = enc.encrypt(data_json)
        data_b64 = b64encode(data_enc)
        data = data_b64.decode(self.encoding)

        return self.create_header(set_encryption) | {
            "Signature": data,
        }

    def get_headers(self, content: str) -> tuple[str, str, str, dict[str, str]]:
        if self.private_key is None:
            raise ValueError("No public remote key available")

        dec = HybridDecryptionAlgorithm(self.private_key, encoding=self.encoding)

        token = content.encode(self.encoding)
        data_b64 = b64decode(token)
        data_enc = dec.decrypt(data_b64)
        data: dict = json.loads(data_enc)

        component_id, checksum, signature = data["id"], data["checksum"], data["signature"]

        extra = {k: v for k, v in data.items() if k not in ("id", "checksum", "signature")}

        return component_id, checksum, signature, extra

    def create_payload(self, content: str | bytes) -> tuple[str, bytes]:
        """Convert a dictionary of a JSON object in string format, then
        encode it for transfer using an hybrid encryption algorithm.

        :param content:
            The dictionary to encrypt.

        :raise:
            ValueError if the remote key is not set.

        :return:
            The headers to use and the data to send.
        """
        if self.remote_key is None:
            raise ValueError("No public remote key available")

        enc = HybridEncryptionAlgorithm(self.remote_key, encoding=self.encoding)

        payload = enc.encrypt(content)
        checksum = enc.get_checksum()

        return checksum, payload

    def get_payload(self, content: str | bytes) -> tuple[str, bytes]:
        """Convert the received content in bytes format to a dictionary
        assuming the content is a JSON object in string format, then
        decode it using an hybrid encryption algorithm.

        :param content:
            The content to decrypt.

        :return:
            A JSON object in dictionary format, the checksum of the data.

        :raise:
            ValueError if the private key is not set.
        """
        if self.private_key is None:
            raise ValueError("No private key available")

        if isinstance(content, str):
            content = content.encode(self.encoding)

        dec = HybridDecryptionAlgorithm(self.private_key, encoding=self.encoding)

        dec_payload = dec.decrypt(content)
        checksum = dec.get_checksum()

        return checksum, dec_payload

    def create(
        self,
        component_id: str,
        content: str | bytes = "",
        set_encryption: bool = True,
        extra_headers: dict[str, str] = dict(),
    ) -> tuple[dict[str, str], bytes]:
        checksum, payload = self.create_payload(content)
        headers = self.create_signed_header(component_id, checksum, set_encryption, extra_headers)

        return headers, payload

    def stream(self, content: str | bytes) -> tuple[str, Iterator[bytes]]:
        """Creates a stream from content in memory.

        :param content:
            The content to stream to the remote host.
        :return:
            An iterator that can be consumed to produce the stream.
        :raise:
            ValueError if the remote host key is not set.
        """
        if self.remote_key is None:
            raise ValueError("No remote key available")

        checksum = str_checksum(content)

        enc = HybridEncryptionAlgorithm(self.remote_key, encoding=self.encoding)

        return checksum, enc.encrypt_content_to_stream(content)

    def stream_from_file(self, path: Path) -> tuple[str, Iterator[bytes]]:
        """Creates a stream from content from a file.

        :param path:
            The path where the content to stream is located.
        :return:
            An iterator that can be consumed to produce the stream.
        :raise:
            ValueError if the remote host key is not set.
        """
        if self.remote_key is None:
            raise ValueError("No remote key available")

        enc = HybridEncryptionAlgorithm(self.remote_key, encoding=self.encoding)

        checksum = file_checksum(path)

        return checksum, enc.encrypt_file_to_stream(path)

    def stream_response(self, content: Iterator[bytes]) -> tuple[bytes, str]:
        """Consumes the stream content of a response, and save the content in memory.

        :param stream:
            A requests.Response opened with the attribute `stream=True`.
        :raise:
            ValueError if no private key is available.
        """
        if self.private_key is None:
            raise ValueError("No private key available")

        dec = HybridDecryptionAlgorithm(self.private_key, encoding=self.encoding)

        data = dec.decrypt_content_stream(content)
        return data, dec.get_checksum()

    def stream_response_to_file(self, stream: Response, path_out: Path, CHUNK_SIZE: int = 4096) -> str:
        """Consumes the stream content of a response, and save the content to file.

        :param stream:
            A requests.Response opened with the attribute `stream=True`.
        :param path:
            Location on disk to save the download content to.
        :raise:
            ValueError if no private key is available or the path already exists.
        """
        if self.private_key is None:
            raise ValueError("No private key available")

        if os.path.exists(path_out):
            raise ValueError(f"path {path_out} already exists")

        dec = HybridDecryptionAlgorithm(self.private_key, encoding=self.encoding)
        dec.decrypt_stream_to_file(stream.iter_content(chunk_size=CHUNK_SIZE), path_out)

        return dec.get_checksum()

    def encrypt_file_for_remote(self, path_in: Path, path_out: Path) -> str:
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

        enc = HybridEncryptionAlgorithm(self.remote_key)
        with open(path_out, "wb") as w:
            w.write(enc.start())
            with open(path_in, "rb") as r:
                while content := r.read():
                    w.write(enc.update(content))
                w.write(enc.end())

        return enc.get_checksum()

    def encrypt_file(self, path_in: Path, path_out: Path) -> str:
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

        enc = HybridEncryptionAlgorithm(self.public_key)
        with open(path_out, "wb") as w:
            w.write(enc.start())
            with open(path_in, "rb") as r:
                while content := r.read():
                    w.write(enc.update(content))
                w.write(enc.end())

        return enc.get_checksum()

    def decrypt_file(self, path_in: Path, path_out: Path) -> None:
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

        enc = HybridDecryptionAlgorithm(self.private_key)
        with open(path_out, "wb") as w:
            w.write(enc.start())
            with open(path_in, "rb") as r:
                while content := r.read():
                    w.write(enc.update(content))
                w.write(enc.end())
