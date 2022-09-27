from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasicCredentials, HTTPBearer

from datetime import timedelta, datetime
from hashlib import sha256
from sqlalchemy.orm import Session
from time import time

from ferdelance_shared.generate import (
    generate_asymmetric_key,
    bytes_from_private_key,
    bytes_from_public_key,
    RSAPublicKey,
    RSAPrivateKey,
)

from ..database import get_db
from ..database.settings import KeyValueStore
from ..database.tables import ClientToken
from .services.client import ClientService

import logging
import os
import uuid

LOGGER = logging.getLogger(__name__)


MAIN_KEY = 'SERVER_MAIN_PASSWORD'
PUBLIC_KEY = 'SERVER_KEY_PUBLIC'
PRIVATE_KEY = 'SERVER_KEY_PRIVATE'


def generate_keys(db: Session) -> None:
    """Initizalization method for the generation of keys for the server.
    Requires to have the environment variable 'SERVER_MAIN_PASSWORD'.

    :param db:
        Current session to the database.
    """

    SMP_VALUE = os.environ.get(MAIN_KEY, None)

    if SMP_VALUE is None:
        LOGGER.fatal(f'Environment variable {MAIN_KEY} is missing.')
        raise ValueError(f'{MAIN_KEY} missing')

    kvs = KeyValueStore(db)

    try:
        db_smp_key = kvs.get_str(MAIN_KEY)

        if db_smp_key != SMP_VALUE:
            LOGGER.fatal(f'Environment variable {MAIN_KEY} invalid: please set the correct password!')
            raise Exception(f'{MAIN_KEY} invalid')

    except ValueError:
        kvs.put_str(MAIN_KEY, SMP_VALUE)
        LOGGER.info(f'Application initialization, Environment variable {MAIN_KEY} saved in storage')

    try:
        private_key = kvs.get_bytes(PRIVATE_KEY)
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

    kvs.put_bytes(PRIVATE_KEY, private_bytes)
    kvs.put_bytes(PUBLIC_KEY, public_bytes)

    LOGGER.info('Keys generation completed')


def generate_token(system: str, mac_address: str, node: str, client_id: str = None, exp_time: int = None) -> ClientToken:
    """Generates a client token with the data received from the client."""

    if client_id is None:
        client_id = str(uuid.uuid4())
        LOGGER.info(f'client_id={client_id}: generating new token')
    else:
        LOGGER.info('generating token for new client')

    ms = round(time() * 1000)

    token: bytes = f'{client_id}~{system}${mac_address}£{node}={ms};'.encode('utf8')
    token: bytes = sha256(token).hexdigest().encode('utf8')
    token: str = sha256(token).hexdigest()

    return ClientToken(
        token=token,
        client_id=client_id,
        expiration_time=exp_time,
    )


def check_token(credentials: HTTPBasicCredentials = Depends(HTTPBearer()), db: Session = Depends(get_db)) -> str:
    """Check if the given token exists in the database.

    :param db:
        Current session to the database.
    :param token:
        Header token received from the client.
    :return:
        True if the token is valid, otherwise false.
    """
    token = credentials.credentials

    cs: ClientService = ClientService(db)

    client_token: ClientToken = cs.get_client_token_by_token(token)

    # TODO: add expiration to token, and also an endpoint to update the token using an expired one

    if client_token is None:
        LOGGER.warning('received token does not exist in database')
        raise HTTPException(401, 'Invalid access token')

    client_id = client_token.client_id

    if not client_token.valid:
        LOGGER.warning('received invalid token')
        raise HTTPException(403, 'Permission denied')

    if client_token.creation_time + timedelta(seconds=client_token.expiration_time) < datetime.now(client_token.creation_time.tzinfo):
        LOGGER.warning(f'client_id={client_id}: received expired token: invalidating')
        db.query(ClientToken).filter(ClientToken.token == client_token.token).update({'valid': False})
        db.commit()
        # allow access only for a single time, since the token update has priority

    LOGGER.info(f'client_id={client_id}: received valid token')
    return client_id
