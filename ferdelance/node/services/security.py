from ferdelance.logging import get_logger
from ferdelance.database.repositories import Repository, AsyncSession
from ferdelance.database.repositories.settings import KeyValueStore
from ferdelance.shared.exchange import Exchange
from ferdelance.shared.decode import HybridDecrypter

from fastapi import Request
from fastapi.responses import StreamingResponse, Response

from typing import Iterator

import aiofiles

LOGGER = get_logger(__name__)


MAIN_KEY = "SERVER_MAIN_PASSWORD"
PUBLIC_KEY = "SERVER_KEY_PUBLIC"
PRIVATE_KEY = "SERVER_KEY_PRIVATE"


class SecurityService(Repository):
    def __init__(self, db: AsyncSession) -> None:
        super().__init__(db)

        self.kvs: KeyValueStore = KeyValueStore(db)
        self.exc: Exchange = Exchange()

    async def setup(self, remote_key_str: str | None = None) -> None:
        private_bytes: bytes = await self.kvs.get_bytes(PRIVATE_KEY)
        self.exc.set_key_bytes(private_bytes)

        if remote_key_str is not None:
            remote_key_bytes = remote_key_str.encode("utf8")
            self.exc.set_remote_key_bytes(remote_key_bytes)

    def get_server_public_key(self) -> str:
        """
        :return:
            The server public key in string format.
        """
        return self.exc.transfer_public_key()

    def get_server_private_key(self) -> str:
        """
        :return:
            The server private key in string format.
        """
        return self.exc.transfer_private_key()

    def encrypt(self, content: str) -> str:
        return self.exc.encrypt(content)

    def decrypt(self, content: str) -> str:
        return self.exc.decrypt(content)

    def verify_headers(self, request: Request) -> tuple[str, str]:
        headers = request.headers.get("Authentication", "")

        if not headers:
            raise ValueError("Invalid header signatures")

        component_id, checksum = self.exc.get_header(headers)

        return component_id, checksum

    def create_response(self, content: bytes) -> Response:
        data = self.exc.create_payload(content)
        return Response(content=data)

    async def read_request(self, request: Request) -> tuple[str, bytes]:
        body = await request.body()
        return self.exc.get_payload(body)

    def encrypt_file(self, path: str) -> StreamingResponse:
        """Used to stream encrypt data from a file, using less memory."""
        _, it = self.exc.stream_from_file(path)
        return StreamingResponse(it, media_type="application/octet-stream")

    async def stream_decrypt_file(self, request: Request, path: str) -> str:
        """Used to stream decrypt data to a file, using less memory."""
        if self.exc.private_key is None:
            raise ValueError("Missing local private key, i exchange object initialized?")

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
