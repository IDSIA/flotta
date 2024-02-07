from typing import AsyncGenerator, Iterator

from ferdelance.security.algorithms import DecryptionAlgorithm, EncryptionAlgorithm, Algorithm
from ferdelance.security.checksums import str_checksum, file_checksum
from ferdelance.security.headers import SignedHeaders
from ferdelance.security.keys.asymmetric import PrivateKey, PublicKey

from base64 import b64decode, b64encode
from pathlib import Path

import json
import os


class Exchange:
    def __init__(
        self,
        source_id: str,
        private_key: PrivateKey | str | bytes | None = None,
        remote_id: str | None = None,
        remote_key: PublicKey | str | bytes | None = None,
        proxy_key: PublicKey | str | bytes | None = None,
        private_key_path: Path | None = None,
        algorithm: Algorithm = Algorithm.HYBRID,
        encoding: str = "utf8",
    ) -> None:
        self.source_id: str = source_id
        self.target_id: str | None = None

        self.private_key: PrivateKey | None = None
        self.public_key: PublicKey | None = None

        self.remote_key: PublicKey | None = None
        self.proxy_key: PublicKey | None = None

        self.algorithm: Algorithm = algorithm
        self.encoding: str = encoding

        if private_key is not None:
            if not isinstance(private_key, PrivateKey):
                private_key = PrivateKey(private_key)

            self.private_key = private_key
            self.public_key = self.private_key.public_key()

        elif private_key_path is not None:
            self.load_private_key(private_key_path)

        else:
            self.generate_keys()

        if remote_key is not None and remote_id is not None:
            self.set_remote_key(remote_id, remote_key)

        if proxy_key is not None:
            self.set_proxy_key(proxy_key)

    # --- key management ----------------

    def generate_keys(self) -> None:
        """Generates a new pair of asymmetric keys."""
        self.private_key = PrivateKey()
        self.public_key = self.private_key.public_key()

    def load_private_key(self, path: Path) -> None:
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

    def set_private_key(self, data: str | bytes) -> None:
        """Set the private key and the public key from a private key
        in string format and encoded for transfer.

        :param data:
            Private key stored in memory and in string encoded for transfer format.
        """
        if isinstance(data, str):
            data = data.encode(self.encoding)

        self.private_key = PrivateKey(data, self.encoding)
        self.public_key = self.private_key.public_key()

    def set_remote_key(self, target_id: str, public_key: PublicKey | str | bytes) -> None:
        """Decode and set a public key from a remote host.

        :param data:
            String content not yet decoded.
        """
        if not isinstance(public_key, PublicKey):
            public_key = PublicKey(public_key, self.encoding)

        self.remote_key = public_key
        self.target_id = target_id

    def set_proxy_key(self, proxy_key: PublicKey | str | bytes) -> None:
        """Decode and set a public key from a proxy host.

        :param data:
            String content not yet decoded.
        """
        if not isinstance(proxy_key, PublicKey):
            proxy_key = PublicKey(proxy_key, self.encoding)

        self.proxy_key = proxy_key

    def clear_proxy(self) -> None:
        self.proxy_key = None

    def transfer_private_key(self) -> str:
        """Encode the stored private key for secure transfer as string.

        :return:
            An encoded private key in string format.
        :raise:
            ValueError if there is no private key available.
        """
        if self.private_key is None:
            raise ValueError("public key not set")

        return self.private_key.bytes().decode(self.encoding)

    def transfer_public_key(self) -> str:
        """Encode the stored public key for secure transfer to remote host.

        :return:
            An encoded public key in string format.
        :raise:
            ValueError if there is no public key available.
        """
        if self.public_key is None:
            raise ValueError("public key not set")

        return self.public_key.bytes().decode(self.encoding)

    def transfer_remote_key(self) -> str:
        """Encode the stored private key for secure transfer as string.

        :return:
            The encoded remote public key in string format.
        :raise:
            ValueError if there is no remote key available.
        """
        if self.remote_key is None:
            raise ValueError("remote public key not set")

        return self.remote_key.bytes().decode(self.encoding)

    def transfer_proxy_key(self) -> str:
        """Encode the stored proxy key for secure transfer as string.

        :return:
            The encoded remote public key in string format.
        :raise:
            ValueError if there is no remote key available.
        """
        if self.proxy_key is None:
            raise ValueError("proxy public key not set")

        return self.proxy_key.bytes().decode(self.encoding)

    # --- body en/decryption with keys --

    def encrypt(self, content: str | bytes) -> bytes:
        """Encrypt a text for the remote host using the stored remote public key.

        :param content:
            Content to be encrypted.
        :raise:
            ValueError if there is no remote public key.
        """
        if self.remote_key is None:
            raise ValueError("No remote host public key available")

        return self.remote_key.encrypt(content, self.encoding)

    def decrypt(self, content: str | bytes) -> bytes:
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

    # --- signatures with keys ----------

    def sign(self, content: str | bytes) -> str:
        if self.private_key is None:
            raise ValueError("Private key not set")

        signature = self.private_key.sign(content, self.encoding)

        return b64encode(signature).decode(self.encoding)

    def verify(self, content: str | bytes, signature: str | bytes) -> None:
        if self.remote_key is None:
            raise ValueError("Remote key not set")

        self.remote_key.verify(content, b64decode(signature), self.encoding)

    # --- headers -----------------------

    def create_signed_headers(
        self,
        checksum: str,
        extra_headers: dict[str, str] = dict(),
        algorithm: Algorithm | None = None,
    ) -> dict[str, str]:
        key: PublicKey

        if self.proxy_key is None:
            if self.remote_key is None:
                raise ValueError("No public remote key available")
            else:
                key = self.remote_key
        else:
            key = self.proxy_key

        if self.target_id is None:
            raise ValueError("No remote target_id set")

        data_to_sign = f"{self.source_id}:{checksum}"
        signature = self.sign(data_to_sign)

        if algorithm:
            algorithm_name = algorithm.name
        else:
            algorithm_name = self.algorithm.name

        header = SignedHeaders(
            source_id=self.source_id,
            target_id=self.target_id,
            checksum=checksum,
            signature=signature,
            encryption=algorithm_name,
            extra=extra_headers,
        )

        enc = Algorithm.HYBRID.enc(key, self.encoding)

        data_json = header.json()
        data_enc = enc.encrypt(data_json)
        data_b64 = b64encode(data_enc)
        data = data_b64.decode(self.encoding)

        return {
            "Signature": data,
        }

    def get_headers(self, content: str) -> SignedHeaders:
        if self.private_key is None:
            raise ValueError("No public remote key available")

        dec = self.algorithm.dec(self.private_key, self.encoding)

        data = content.encode(self.encoding)
        data_b64 = b64decode(data)
        dec_data = dec.decrypt(data_b64)
        return SignedHeaders(**json.loads(dec_data))

    def create_payload(self, content: str | bytes) -> tuple[str, bytes]:
        """Convert a dictionary of a JSON object in string format, then
        encode it for transfer using an hybrid encryption algorithm.

        :param content:
            The dictionary to encrypt.

        :raise:
            ValueError if the remote key is not set.

        :return:
            The checksum of teh data and the data to send.
        """
        if self.remote_key is None:
            raise ValueError("No public remote key available")

        enc: EncryptionAlgorithm = self.algorithm.enc(self.remote_key, self.encoding)

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

        dec: DecryptionAlgorithm = self.algorithm.dec(self.private_key, self.encoding)

        dec_payload = dec.decrypt(content)
        checksum = dec.get_checksum()

        return checksum, dec_payload

    def create(
        self,
        content: str | bytes = "",
        extra_headers: dict[str, str] = dict(),
    ) -> tuple[dict[str, str], bytes]:
        checksum, payload = self.create_payload(content)
        headers = self.create_signed_headers(checksum, extra_headers)

        return headers, payload

    # --- data streaming ----------------

    def encrypt_to_stream(self, content: str | bytes) -> tuple[str, Iterator[bytes]]:
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

        enc = self.algorithm.enc(self.remote_key, self.encoding)

        return checksum, enc.encrypt_content_to_stream(content)

    def encrypt_file_to_stream(self, path: Path) -> tuple[str, Iterator[bytes]]:
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

        enc = self.algorithm.enc(self.remote_key, self.encoding)

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

        dec = self.algorithm.dec(self.private_key, self.encoding)

        data = dec.decrypt_stream(content)
        return data, dec.get_checksum()

    def stream_response_to_file(self, stream: Iterator[bytes], path_out: Path) -> str:
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

        dec = self.algorithm.dec(self.private_key, self.encoding)
        return dec.decrypt_stream_to_file(stream, path_out)

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

        enc = self.algorithm.enc(self.remote_key, self.encoding)
        return enc.encrypt_file(path_in, path_out)

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

        enc = self.algorithm.enc(self.public_key, self.encoding)
        return enc.encrypt_file(path_in, path_out)

    def decrypt_file(self, path_in: Path, path_out: Path) -> str:
        """Decrypt a file from disk to another file on disk. This file can must be
        encrypted with a valid public key.

        :param path_in:
            Source file to decrypt.
        :param path_out:
            Destination path of the decrypted file.
        :raise:
            ValueError if there is no private key available.

        :return:
            Checksum of the decrypted file.
        """

        if self.private_key is None:
            raise ValueError("No private key available")

        dec = self.algorithm.dec(self.private_key, self.encoding)
        return dec.decrypt_file(path_in, path_out)

    def stream_decrypt(self, stream: Iterator[bytes]) -> tuple[str, bytes]:
        if self.private_key is None:
            raise ValueError("No private key available")

        dec = self.algorithm.dec(self.private_key, self.encoding)

        data = dec.decrypt_stream(stream)

        return dec.get_checksum(), data

    async def stream_decrypt_file(self, stream: AsyncGenerator[bytes, None], path_out: Path) -> str:
        if self.private_key is None:
            raise ValueError("No private key available")

        dec = self.algorithm.dec(self.private_key, self.encoding)
        return await dec.decrypt_stream_to_file_a(stream, path_out)
