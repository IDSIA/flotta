from fastapi import Request
from fastapi.responses import StreamingResponse, Response

from ferdelance.shared.exchange import Exchange

from ferdelance.shared.decode import HybridDecrypter

from ...database.services import (
    DBSessionService,
    AsyncSession,
)
from ...database.services.settings import KeyValueStore

from typing import Any, Iterator

import aiofiles
import logging

LOGGER = logging.getLogger(__name__)


MAIN_KEY = 'SERVER_MAIN_PASSWORD'
PUBLIC_KEY = 'SERVER_KEY_PUBLIC'
PRIVATE_KEY = 'SERVER_KEY_PRIVATE'


class SecurityService(DBSessionService):
    def __init__(self, db: AsyncSession) -> None:
        super().__init__(db)

        self.kvs: KeyValueStore = KeyValueStore(db)
        self.exc: Exchange = Exchange()

    async def setup(self, remote_key_str: str) -> None:
        remote_key_bytes = remote_key_str.encode('utf8')
        private_bytes: bytes = await self.kvs.get_bytes(PRIVATE_KEY)
        self.exc.set_key_bytes(private_bytes)
        self.exc.set_remote_key_bytes(remote_key_bytes)

    def get_server_public_key(self) -> str:
        """
        :return:
            The server public key in string format.
        """
        return self.exc.transfer_public_key()

    def encrypt(self, content: str) -> str:
        return self.exc.encrypt(content)

    def decrypt(self, content: str) -> str:
        return self.exc.decrypt(content)

    def create_response(self, content: dict[str, Any]) -> Response:
        data = self.exc.create_payload(content)
        return Response(content=data)

    async def read_request(self, request: Request) -> dict[str, Any]:
        body = await request.body()
        return self.exc.get_payload(body)

    def encrypt_file(self, path: str) -> StreamingResponse:
        """Used to stream encrypt data from a file, using less memory."""
        return StreamingResponse(
            self.exc.stream_from_file(path),
            media_type='application/octet-stream'
        )

    async def stream_decrypt_file(self, request: Request, path: str) -> str:
        """Used to stream decrypt data to a file, using less memory."""
        if self.exc.private_key is None:
            raise ValueError('Missing local private key, i exchange object initialized?')

        dec = HybridDecrypter(self.exc.private_key)

        async with aiofiles.open(path, 'wb') as f:
            await f.write(dec.start())
            async for content in request.stream():
                await f.write(dec.update(content))
            await f.write(dec.end())

        return dec.get_checksum()

    def stream_encrypt(self, content: str) -> Iterator[bytes]:
        """Used to encrypt small data that can be kept in memory."""
        return self.exc.stream(content)

    async def stream_decrypt(self, request: Request) -> tuple[str, str]:
        """Used to decrypt small data that can be kept in memory."""
        if self.exc.private_key is None:
            raise ValueError('Missing local private key, i exchange object initialized?')

        dec = HybridDecrypter(self.exc.private_key)

        data: bytearray = bytearray()
        data.extend(dec.start())
        async for content in request.stream():
            data.extend(dec.update(content))
        data.extend(dec.end())

        return data.decode(dec.encoding), dec.get_checksum()
