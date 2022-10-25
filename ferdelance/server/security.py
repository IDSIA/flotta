from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasicCredentials, HTTPBearer

from datetime import timedelta, datetime

from ferdelance_shared.generate import (
    generate_asymmetric_key,
    bytes_from_private_key,
    bytes_from_public_key,
    RSAPublicKey,
    RSAPrivateKey,
)

from ..database import get_session, AsyncSession
from ..database.services import KeyValueStore
from ..database.tables import ClientToken
from ..database.services.client import ClientService

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
        LOGGER.fatal(f'Environment variable {MAIN_KEY} is missing.')
        raise ValueError(f'{MAIN_KEY} missing')

    kvs = KeyValueStore(session)

    try:
        db_smp_key = kvs.get_str(MAIN_KEY)

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

    private_key: RSAPrivateKey = generate_asymmetric_key()
    public_key: RSAPublicKey = private_key.public_key()

    private_bytes: bytes = bytes_from_private_key(private_key)
    public_bytes: bytes = bytes_from_public_key(public_key)

    await kvs.put_bytes(PRIVATE_KEY, private_bytes)
    await kvs.put_bytes(PUBLIC_KEY, public_bytes)

    LOGGER.info('Keys generation completed')


async def check_token(credentials: HTTPBasicCredentials = Depends(HTTPBearer()), session: AsyncSession = Depends(get_session)) -> str:
    """Check if the given token exists in the database.

    :param db:
        Current session to the database.
    :param token:
        Header token received from the client.
    :return:
        True if the token is valid, otherwise false.
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

    return client_id
