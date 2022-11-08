from typing import Any, Iterator
from .generate import (
    generate_asymmetric_key,
    bytes_from_private_key,
    bytes_from_public_key,
    private_key_from_bytes,
    public_key_from_str,
    RSAPrivateKey,
    RSAPublicKey,
)
from .decode import (
    HybridDecrypter,
    decode_from_transfer,
)
from .encode import (
    HybridEncrypter,
    encode_to_transfer,
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
            raise ValueError(f'SSH key file {path} does not exists')

        with open(path, 'rb') as f:
            pk_bytes: bytes = f.read()
            self.private_key = private_key_from_bytes(pk_bytes)
            self.public_key = self.private_key.public_key()

    def save_private_key(self, path: str) -> None:
        """Save the stored private key to disk.

        :param path:
            Location of the private key to save to.
        :raise:
            ValueError if the path already exists or the private key
            is not available.
        """
        if os.path.exists(path):
            raise ValueError(f'destination path {path} already exists')

        if self.private_key is None:
            raise ValueError('cannot save: no private key available')

        with open(os.path.join(path, 'rsa_id'), 'wb') as f:
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
            raise ValueError(f'destination path {path} already exists')

        if self.public_key is None:
            raise ValueError('cannot save: no public key available')

        with open(os.path.join(path, 'rsa_id'), 'wb') as f:
            pk_bytes: bytes = bytes_from_public_key(self.public_key)
            f.write(pk_bytes)

    def set_token(self, token: str) -> None:
        """Set the token for authentication.

        :param token:
            Token to use.
        """
        self.token = token

    def set_remote_key(self, data: str) -> None:
        """Decode and set a public key from a remote host.

        :param data:
            String content not yet decoded.
        """
        self.remote_key = public_key_from_str(decode_from_transfer(data))

    def transfer_public_key(self):
        if self.public_key is None:
            raise ValueError('public key not set')

        data: str = bytes_from_public_key(self.public_key).decode('utf8')
        return encode_to_transfer(data)

    def headers(self) -> dict[str, str]:
        """Build headers for authentication.
        :return: 
            The headers to use for authentication.
        :raise:
            ValueError if not token is available.
        """
        if self.token is None:
            raise ValueError('token not set')

        return {
            'Authorization': f'Bearer {self.token}'
        }

    def create_payload(self, content: dict[str, Any]) -> bytes:
        """Convert a dictionary in a JSON object in string format, then 
        encode it for transfer using an hybrid encryption algorithm.

        :param content:
            The dictionary to encrypt.
        :raise:
            ValueError if the remote key is not set.
        """
        if self.remote_key is None:
            raise ValueError('No public remote key available')

        payload: str = json.dumps(content)

        return HybridEncrypter(self.remote_key).encrypt(payload)

    def get_payload(self, content: bytes) -> dict[str, Any]:
        """Convert the received content in bytes format to a dictionary 
        assuming the content is a JSON object in string format, then 
        decode it using an hybrid encryption algorithm.

        :param content:
            The content to decrypt.
        :return:
            A JSON object in dictionary format.
        :raise:
            ValueError if the private key is not set.
        """
        if self.private_key is None:
            raise ValueError('No private key available')

        return json.loads(
            HybridDecrypter(self.private_key).decrypt(content)
        )

    def stream(self, content: str) -> Iterator[bytes]:
        """Creates a stream from content in memory.

        :param content:
            The content to stream to the remote host.
        :return:
            An iterator that can be consumed to produce the stream.
        :raise:
            ValueError if the remote host key is not set.
        """
        if self.remote_key is None:
            raise ValueError('No remote key available')

        enc = HybridEncrypter(self.remote_key)

        return enc.encrypt_to_stream(content)

    def stream_from_file(self, path: str) -> Iterator[bytes]:
        """Creates a stream from content from a file.

        :param path:
            The path where the content to stream is located.
        :return:
            An iterator that can be consumed to produce the stream.
        :raise:
            ValueError if the remote host key is not set.
        """
        if self.remote_key is None:
            raise ValueError('No remote key available')

        enc = HybridEncrypter(self.remote_key)

        return enc.encrypt_file_to_stream(path)

    def stream_response(self, stream: Response) -> tuple[str, str]:
        """Consumes the stream content of a response, and save the content in memory.

        :param stream:
            A requests.Response opened with the attribute `stream=True`.
        :raise:
            ValueError if no private key is available.
        """
        if self.private_key is None:
            raise ValueError('No private key available')

        dec = HybridDecrypter(self.private_key)

        data = dec.decrypt_stream(stream.iter_content())
        return data, dec.get_checksum()

    def stream_response_to_file(self, stream: Response, path: str) -> str:
        """Consumes the stream content of a response, and save the content to file.

        :param stream:
            A requests.Response opened with the attribute `stream=True`.
        :param path:
            Location on disk to save the download content to.
        :raise:
            ValueError if no private key is available or the path already exists.
        """
        if self.private_key is None:
            raise ValueError('No private key available')

        if os.path.exists(path):
            raise ValueError(f'path {path} already exists')

        dec = HybridDecrypter(self.private_key)
        dec.decrypt_stream_to_file(stream.iter_content(), path)

        return dec.get_checksum()
