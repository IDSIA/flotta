from ferdelance.database.tables import Token
from ferdelance.database.services import DBSessionService, AsyncSession
from ferdelance.database.services.settings import KeyValueStore, KEY_CLIENT_TOKEN_EXPIRATION, KEY_USER_TOKEN_EXPIRATION

from sqlalchemy import select

from hashlib import sha256
from uuid import uuid4
from time import time

import logging

LOGGER = logging.getLogger(__name__)


class TokenService(DBSessionService):
    """This is an internal service used by ComponentService."""

    def __init__(self, session: AsyncSession, encoding: str = "utf8") -> None:
        super().__init__(session)

        self.kvs: KeyValueStore = KeyValueStore(session)
        self.encoding: str = encoding

    async def generate_client_token(self, system: str, mac_address: str, node: str, client_id: str = "") -> Token:
        """Generates a client token with the data received from the client."""
        if client_id == "":
            client_id = str(uuid4())
            LOGGER.info(f"client_id={client_id}: generating new token")
        else:
            LOGGER.info("generating token for new client")

        ms = round(time() * 1000)

        token_b: bytes = f"{client_id}~{system}${mac_address}£{node}={ms};".encode(self.encoding)
        token_b: bytes = sha256(token_b).hexdigest().encode(self.encoding)
        token: str = sha256(token_b).hexdigest()

        exp_time: int = await self.kvs.get_int(KEY_CLIENT_TOKEN_EXPIRATION)

        return Token(
            token=token,
            component_id=client_id,
            expiration_time=exp_time,
        )

    async def generate_token(self, user_id: str = "") -> Token:
        """Generates a user token."""
        if user_id == "":
            user_id = str(uuid4())
            LOGGER.info(f"user_id={user_id}: generating new token")
        else:
            LOGGER.info("generating token for new client")

        ms = round(time() * 1000)
        salt = str(uuid4())[:16]

        token_b: bytes = f"{user_id}~{salt}={ms};".encode(self.encoding)
        token_b: bytes = sha256(token_b).hexdigest().encode(self.encoding)
        token: str = sha256(token_b).hexdigest()

        exp_time: int = await self.kvs.get_int(KEY_USER_TOKEN_EXPIRATION)

        return Token(
            token=token,
            component_id=user_id,
            expiration_time=exp_time,
        )

    async def project_token(self, name: str) -> str:
        LOGGER.info("generating token for new project")

        ms = round(time() * 1000 + 7)
        salt = str(uuid4())[:17]

        token_b: bytes = f"{ms}¨{name}${salt};".encode(self.encoding)
        token_b: bytes = sha256(token_b).hexdigest().encode(self.encoding)
        token: str = sha256(token_b).hexdigest()

        return token

    async def create_token(self, token: Token) -> None:
        """Does not commit!"""
        LOGGER.info(f"component_id={token.component_id}: creating new token")

        res = await self.session.execute(select(Token).where(Token.token == token.token))

        existing_token: Token | None = res.scalar_one_or_none()

        if existing_token is not None:
            LOGGER.warning(f"component_id={existing_token.component_id}: a valid token already exists")
            # TODO: check if we have more strong condition for this
            return existing_token

        self.session.add(token)

    async def invalidate_tokens(self, component_id: str) -> None:
        res = await self.session.scalars(select(Token).where(Token.component_id == component_id))
        tokens: list[Token] = res.all()

        for token in tokens:
            token.valid = False

        await self.session.commit()

    async def update_client_token(self, system: str, mac_address: str, node: str, component_id: str = "") -> Token:
        token: Token = await self.generate_client_token(
            system,
            mac_address,
            node,
            component_id,
        )
        await self.invalidate_tokens(component_id)
        await self.create_token(token)
        await self.session.commit()
        await self.session.refresh(token)

        return token
