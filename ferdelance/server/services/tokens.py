from ...database.tables import (
    ClientToken,
    UserToken,
)
from ...database.services import DBSessionService, AsyncSession

from ...database.services.settings import (
    KeyValueStore,
    KEY_CLIENT_TOKEN_EXPIRATION,
    KEY_USER_TOKEN_EXPIRATION
)

from hashlib import sha256
from uuid import uuid4
from time import time

import logging

LOGGER = logging.getLogger(__name__)


class TokenService(DBSessionService):

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

        self.kvs: KeyValueStore = KeyValueStore(session)

    async def generate_client_token(self, system: str, mac_address: str, node: str, client_id: str = '') -> ClientToken:
        """Generates a client token with the data received from the client."""
        if client_id == '':
            client_id = str(uuid4())
            LOGGER.info(f'client_id={client_id}: generating new token')
        else:
            LOGGER.info('generating token for new client')

        ms = round(time() * 1000)

        token_b: bytes = f'{client_id}~{system}${mac_address}£{node}={ms};'.encode('utf8')
        token_b: bytes = sha256(token_b).hexdigest().encode('utf8')
        token: str = sha256(token_b).hexdigest()

        exp_time: int = await self.kvs.get_int(KEY_CLIENT_TOKEN_EXPIRATION)

        return ClientToken(
            token=token,
            client_id=client_id,
            expiration_time=exp_time,
        )

    async def generate_user_token(self, user_id: str = '') -> UserToken:
        """Generates a user token."""
        if user_id == '':
            user_id = str(uuid4())
            LOGGER.info(f'user_id={user_id}: generating new token')
        else:
            LOGGER.info('generating token for new client')

        ms = round(time() * 1000)
        salt = str(uuid4())[:16]

        token_b: bytes = f'{user_id}~{salt}={ms};'.encode('utf8')
        token_b: bytes = sha256(token_b).hexdigest().encode('utf8')
        token: str = sha256(token_b).hexdigest()

        exp_time: int = await self.kvs.get_int(KEY_USER_TOKEN_EXPIRATION)

        return UserToken(
            token=token,
            user_id=user_id,
            expiration_time=exp_time,
        )
