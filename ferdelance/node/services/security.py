from typing import Iterator

from ferdelance.config import config_manager
from ferdelance.logging import get_logger
from ferdelance.shared.checksums import file_checksum
from ferdelance.shared.exchange import Exchange
from ferdelance.shared.decode import HybridDecrypter

from fastapi import Request
from fastapi.responses import StreamingResponse, Response

from pathlib import Path

import aiofiles
import os

LOGGER = get_logger(__name__)


MAIN_KEY = "SERVER_MAIN_PASSWORD"
PUBLIC_KEY = "SERVER_KEY_PUBLIC"
PRIVATE_KEY = "SERVER_KEY_PRIVATE"


class SecurityService:
    def __init__(self, remote_key_str: str | None = None, encoding: str = "utf8") -> None:
        """Creates a secure context between a local node, indicated by the
        `self_component`, and a remote node.

        Args:
            remote_key_str (str | None, optional):
                If present, set the remote public key to use for the connection.
                Otherwise, use the #set_remote_key() method to set the remote key.
                Defaults to None.
        """
        self.encoding = encoding

        private_key_path = config_manager.get().private_key_location()

        self.exc: Exchange = Exchange(private_key_path, self.encoding)

        if remote_key_str is not None:
            self.set_remote_key(remote_key_str)

    def set_remote_key(self, remote_key_str: str) -> None:
        self.exc.set_remote_key(remote_key_str)

    def get_public_key(self) -> str:
        """
        :return:
            The public key of this node in transferrable format.
        """
        return self.exc.transfer_public_key()

    def get_private_key(self) -> str:
        """
        :return:
            The private key of this node in transferrable format.
        """
        return self.exc.transfer_private_key()

    def get_remote_key(self) -> str:
        """
        :return:
            The public key of the remote node in transferrable format.
        """
        return self.exc.transfer_remote_key()

    def sign(self, content: str) -> str:
        return self.exc.sign(content)

    def encrypt(self, content: str) -> str:
        return self.exc.encrypt(content)

    def decrypt(self, content: str) -> str:
        return self.exc.decrypt(content)

    def get_headers(self, signature_data: str) -> tuple[str, str, str, dict[str, str]]:
        return self.exc.get_headers(signature_data)

    def create(
        self,
        component_id: str,
        content: str | bytes = "",
        set_encryption: bool = True,
    ) -> tuple[dict[str, str], bytes]:
        return self.exc.create(component_id, content, set_encryption)

    def verify_signature_data(self, component_id: str, checksum: str, signature: str) -> None:
        self.exc.verify(f"{component_id}:{checksum}", signature)

    def create_response(self, content: bytes) -> Response:
        data = self.exc.create_payload(content)
        return Response(content=data)

    async def read_request(self, request: Request) -> tuple[str, bytes]:
        body = await request.body()
        return self.exc.get_payload(body)

    def encrypt_file(self, path: Path) -> tuple[str, StreamingResponse]:
        """Used to stream encrypt data from a file, using less memory."""
        checksum, it = self.exc.stream_from_file(path)
        return checksum, StreamingResponse(it, media_type="application/octet-stream")

    async def stream_decrypt_file(self, request: Request, path: Path) -> str:
        """Used to stream decrypt data to a file, using less memory."""
        if self.exc.private_key is None:
            raise ValueError("Missing local private key, i exchange object initialized?")

        if os.path.exists(path):
            LOGGER.info("destination path already exists")
            return file_checksum(path)

        dec = HybridDecrypter(self.exc.private_key)

        async with aiofiles.open(path, "wb") as f:
            await f.write(dec.start())
            async for content in request.stream():
                await f.write(dec.update(content))
            await f.write(dec.end())

        return dec.get_checksum()

    def stream_encrypt(self, content: str) -> tuple[str, Iterator[bytes]]:
        """Used to encrypt small data that can be kept in memory."""
        return self.exc.stream(content)

    async def stream_decrypt(self, request: Request) -> tuple[str, str]:
        """Used to decrypt small data that can be kept in memory."""
        if self.exc.private_key is None:
            raise ValueError("Missing local private key, i exchange object initialized?")

        dec = HybridDecrypter(self.exc.private_key)

        data: bytearray = bytearray()
        data.extend(dec.start())
        async for content in request.stream():
            data.extend(dec.update(content))
        data.extend(dec.end())

        return data.decode(dec.encoding), dec.get_checksum()
