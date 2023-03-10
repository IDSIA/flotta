from ferdelance.database.tables import Token
from ferdelance.database.repositories import Repository, AsyncSession
from ferdelance.database.repositories.settings import (
    KeyValueStore,
    KEY_CLIENT_TOKEN_EXPIRATION,
    KEY_USER_TOKEN_EXPIRATION,
)

from sqlalchemy import select, func

from hashlib import sha256
from uuid import uuid4
from time import time

import logging

LOGGER = logging.getLogger(__name__)


class TokenRepository(Repository):
    """This repository creates and manages tokens for all component types.

    A token is substantially a string object. In this case, we wrap the
    string with an object that keep tracks of the validity of the token.

    This is an internal repository used by the ComponentRepository and
    should not be created outside of it.
    """

    def __init__(self, session: AsyncSession, encoding: str = "utf8") -> None:
        super().__init__(session)

        self.kvs: KeyValueStore = KeyValueStore(session)
        self.encoding: str = encoding

    async def generate_client_token(self, system: str, mac_address: str, node: str, client_id: str = "") -> Token:
        """Client tokens are based on the information received by the client,
        such as system, hardware address, and uniq machine identifier. This
        method can be used both to generate a first token for a new client or
        re-generate a token for an existing one.

        Args:
            system (str):
                Specifies the operative system of the client.
            mac_address (str):
                Specifies the current hardware address use by the client.
            node (str):
                Unique identifier created by the client.
            client_id (str, optional):
                If already exists, the unique identifier of the client.
                Don't use if the client is a new one (leave default).
                Defaults to "".

        Returns:
            Token:
                Token generated with the input data.
        """
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

    async def generate_user_token(self, user_id: str = "") -> Token:
        """User (or workbench) tokens are based on the id of the user. This
        method can be used both to generate a first token for a new user or
        re-generate a token for an existing one.

        Args:
            user_id (str, optional):
                If already exists, the unique identifier of the user.
                Don't use if the user is a new one (leave default).
                Defaults to "".

        Returns:
            Token:
                Token generated with the input data.
        """
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

    async def generate_project_token(self, name: str) -> str:
        """Project have tokens assigned to them. The token is based on the name
        of the project. Project's tokens are intended to not be replaceable.

        Args:
            name (str):
                The name of the new project.

        Returns:
            str:
                A string to use as a token.
        """

        LOGGER.info("generating token for new project")

        ms = round(time() * 1000 + 7)
        salt = str(uuid4())[:17]

        token_b: bytes = f"{ms}¨{name}${salt};".encode(self.encoding)
        token_b: bytes = sha256(token_b).hexdigest().encode(self.encoding)
        token: str = sha256(token_b).hexdigest()

        return token

    async def invalidate_tokens(self, component_id: str) -> None:
        """Set to not valid all the tokens associated with the given component id.
        This is useful when there is the need to update the token for the client
        or to ban an user.

        Args:
            component_id (str):
                Id of the component.
        """
        res = await self.session.scalars(select(Token).where(Token.component_id == component_id))
        tokens: list[Token] = list(res.all())

        for token in tokens:
            token.valid = False

        await self.session.commit()

    async def update_client_token(self, system: str, mac_address: str, node: str, client_id: str) -> Token:
        """Utility method that creates a new token for the client and set it as
        the new valid token.

        Args:
            system (str):
                Specifies the operative system of the client.
            mac_address (str):
                Specifies the current hardware address use by the client.
            node (str):
                Unique identifier created by the client.
            client_id (str, optional):
                The unique identifier of the client.

        Returns:
            Token:
                The new token to use.
        """
        token: Token = await self.generate_client_token(
            system,
            mac_address,
            node,
            client_id,
        )
        await self.invalidate_tokens(client_id)

        self.session.add(token)

        await self.session.commit()
        await self.session.refresh(token)

        return token

    async def count_valid_tokens(self, client_id: str) -> int:
        """Count how many valid tokens are recorded in the database for the given
        client_id. Normally, only one token marked as valid should be available.
        In case no tokens are recorded, the count is set to zero.

        Args:
            client_id (str):
                Client unique identifier.

        Returns:
            int
                The number of valid tokens recorded for the given client_id.
        """

        n_tokens = await self.session.scalar(
            select(func.count()).select_from(Token).where(Token.component_id == client_id, Token.valid)
        )

        if n_tokens is None:
            return 0

        return n_tokens
