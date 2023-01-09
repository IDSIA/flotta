from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasicCredentials, HTTPBearer

from datetime import timedelta, datetime

from ferdelance.shared.exchange import Exchange

from ..database import get_session, AsyncSession
from ..database.services import KeyValueStore, ClientService, UserService
from ..database.schemas import Client, User
from ..database.tables import ClientToken, UserToken

import logging
import os

LOGGER = logging.getLogger(__name__)


MAIN_KEY = 'SERVER_MAIN_PASSWORD'
PUBLIC_KEY = 'SERVER_KEY_PUBLIC'
PRIVATE_KEY = 'SERVER_KEY_PRIVATE'


async def generate_keys(session: AsyncSession) -> None:
    """Initialization method for the generation of keys for the server.
    Requires to have the environment variable 'SERVER_MAIN_PASSWORD'.

    :param db:
        Current session to the database.
    """

    SMP_VALUE = os.environ.get(MAIN_KEY, None)

    if SMP_VALUE is None:
        LOGGER.critical(f'Environment variable {MAIN_KEY} is missing.')
        raise ValueError(f'{MAIN_KEY} missing')

    kvs = KeyValueStore(session)
    e = Exchange()

    try:
        db_smp_key = await kvs.get_str(MAIN_KEY)

        if db_smp_key != SMP_VALUE:
            LOGGER.fatal(f'Environment variable {MAIN_KEY} invalid: please set the correct password!')
            raise Exception(f'{MAIN_KEY} invalid')

    except ValueError:
        await kvs.put_str(MAIN_KEY, SMP_VALUE)
        LOGGER.info(f'Application initialization, Environment variable {MAIN_KEY} saved in storage')

    try:
        await kvs.get_bytes(PRIVATE_KEY)
        LOGGER.info('Keys are already available')
        return

    except ValueError:
        pass

    # generate new keys
    LOGGER.info('Keys generation started')

    e.generate_key()

    private_bytes: bytes = e.get_private_key_bytes()
    public_bytes: bytes = e.get_public_key_bytes()

    await kvs.put_bytes(PRIVATE_KEY, private_bytes)
    await kvs.put_bytes(PUBLIC_KEY, public_bytes)

    LOGGER.info('Keys generation completed')


async def check_client_token(credentials: HTTPBasicCredentials = Depends(HTTPBearer()), session: AsyncSession = Depends(get_session)) -> Client:
    """Checks if the given client token exists in the database.

    :param credentials:
        Content of Authorization header.
    :session:
        Session on the database.
    :return:
        The client_id associated with the authorization header, otherwise an exception is raised.
    """
    token: str = credentials.credentials  # type: ignore

    cs: ClientService = ClientService(session)

    client_token: ClientToken | None = await cs.get_client_token_by_token(token)

    # TODO: add expiration to token, and also an endpoint to update the token using an expired one

    if client_token is None:
        LOGGER.warning('received token does not exist in database')
        raise HTTPException(401, 'Invalid access token')

    client_id = str(client_token.client_id)

    if not client_token.valid:
        LOGGER.warning('received invalid token')
        raise HTTPException(403, 'Permission denied')

    if client_token.creation_time + timedelta(seconds=client_token.expiration_time) < datetime.now(client_token.creation_time.tzinfo):
        LOGGER.warning(f'client_id={client_id}: received expired token: invalidating')
        await cs.invalidate_all_tokens(client_id)
        # allow access only for a single time, since the token update has priority

    LOGGER.debug(f'client_id={client_id}: received valid token')

    try:

        client = await cs.get_client_by_id(client_id)

        if client.left or not client.active:
            LOGGER.warning('Client that left or has been deactivated tried to connect!')
            raise HTTPException(403, 'Permission denied')

        return client

    except ValueError as e:
        LOGGER.warning(f'valid token does not have client! {e}')
        raise HTTPException(403, 'Permission denied')


async def check_user_token(credentials: HTTPBasicCredentials = Depends(HTTPBearer()), session: AsyncSession = Depends(get_session)) -> User:
    """Checks if the given user token exists in the database.

    :param credentials:
        Content of Authorization header.
    :session:
        Session on the database.
    :return:
        The user_id associated with the authorization header, otherwise an exception is raised.
    """
    token: str = credentials.credentials  # type: ignore

    us: UserService = UserService(session)

    user_token: UserToken | None = await us.get_user_token_by_token(token)

    # TODO: add expiration to token, and also an endpoint to update the token using an expired one

    if user_token is None:
        LOGGER.warning('received token does not exist in database')
        raise HTTPException(401, 'Invalid access token')

    user_id = str(user_token.user_id)

    if not user_token.valid:
        LOGGER.warning('received invalid token')
        raise HTTPException(403, 'Permission denied')

    if user_token.creation_time + timedelta(seconds=user_token.expiration_time) < datetime.now(user_token.creation_time.tzinfo):
        LOGGER.warning(f'user_id={user_id}: received expired token: invalidating')
        await us.invalidate_all_tokens(user_id)
        # allow access only for a single time, since the token update has priority

    LOGGER.debug(f'user_id={user_id}: received valid token')

    try:
        user = await us.get_user_by_id(user_id)

        if user.left or not user.active:
            LOGGER.warning('User that left or has been deactivated tried to connect!')
            raise HTTPException(403, 'Permission denied')

        return user
    except ValueError as e:
        LOGGER.warning(f'valid token does not have user! {e}')
        raise HTTPException(403, 'Permission denied')
