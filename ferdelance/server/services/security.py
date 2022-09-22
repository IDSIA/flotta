from fastapi import UploadFile
from fastapi.responses import StreamingResponse

from ferdelance_shared.generate import (
    public_key_from_bytes,
    private_key_from_bytes,
    RSAPublicKey,
    RSAPrivateKey,
)
from ferdelance_shared.decode import decode_from_transfer, decrypt, HybridDecrypter
from ferdelance_shared.encode import encode_to_transfer, encrypt, HybridEncrypter

from ...database.settings import KeyValueStore
from ...database.tables import Client
from ..schemas.client import ClientJoinRequest
from . import DBSessionService, Session

import logging

LOGGER = logging.getLogger(__name__)


MAIN_KEY = 'SERVER_MAIN_PASSWORD'
PUBLIC_KEY = 'SERVER_KEY_PUBLIC'
PRIVATE_KEY = 'SERVER_KEY_PRIVATE'


class SecurityService(DBSessionService):
    def __init__(self, db: Session) -> None:
        super().__init__(db)

        self.kvs: KeyValueStore = KeyValueStore(db)

    def get_server_public_key(self) -> RSAPublicKey:
        """
        :return:
            The server public key in string format.
        """
        public_bytes: bytes = self.kvs.get_bytes(PUBLIC_KEY)
        return public_key_from_bytes(public_bytes)

    def get_server_public_key_str(self) -> str:
        public_str: bytes = self.kvs.get_str(PUBLIC_KEY)
        return encode_to_transfer(public_str)

    def get_server_private_key(self) -> str:
        """
        :param db:
            Current session to the database.
        :return:
            The server public key in string format.
        """
        private_bytes: bytes = self.kvs.get_bytes(PRIVATE_KEY)
        return private_key_from_bytes(private_bytes)

    def get_client_public_key(self, client: Client | ClientJoinRequest) -> RSAPublicKey:
        key_bytes: bytes = decode_from_transfer(client.public_key).encode('utf8')
        public_key: RSAPublicKey = public_key_from_bytes(key_bytes)
        return public_key

    def server_encrypt(self, client: Client, content: str) -> str:
        client_public_key: RSAPublicKey = self.get_client_public_key(client)
        return encrypt(client_public_key, content)

    def server_decrypt(self, content: str) -> str:
        server_private_key: RSAPrivateKey = self.get_server_private_key()
        return decrypt(server_private_key, content)

    def server_stream_encrypt_file(self, client: Client, path: str) -> StreamingResponse:
        """Used to stream encrypt data from a file, using less memory."""
        client_public_key: RSAPublicKey = self.get_client_public_key(client)

        enc = HybridEncrypter(client_public_key)

        return StreamingResponse(
            enc.encrypt_file_to_stream(path),
            media_type='application/octet-stream'
        )

    def server_stream_decrypt_file(self, file: UploadFile, path: str) -> None:
        """Used to stream decrypt data to a file, using less memory."""
        private_key: RSAPrivateKey = self.get_server_private_key()

        dec = HybridDecrypter(private_key)

        dec.decrypt_stream_to_file(iter(file.file), path)

        # TODO: find a way to add the checksum to the stream

    def server_stream_encrypt(self, client: Client, content: str) -> bytes:
        """Used to encrypt small data that can be kept in memory."""
        public_key = self.get_client_public_key(client)

        enc = HybridEncrypter(public_key)

        return enc.encrypt_to_stream(content)

    def server_stream_decrypt(self, file: UploadFile) -> str:
        """Used to decrypt small data that can be kept in memory."""
        private_key: RSAPrivateKey = self.get_server_private_key()

        dec = HybridDecrypter(private_key)
        return dec.decrypt_stream(iter(file.file))

        # TODO: find a way to add the checksum to the stream
