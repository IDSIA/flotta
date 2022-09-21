from fastapi import Depends, HTTPException, UploadFile
from fastapi.security import HTTPBasicCredentials, HTTPBearer
from fastapi.responses import StreamingResponse

from datetime import timedelta, datetime
from hashlib import sha256
from sqlalchemy.orm import Session
from time import time

from ferdelance_shared.generate import (
    generate_asymmetric_key,
    bytes_from_private_key,
    bytes_from_public_key,
    public_key_from_bytes,
    private_key_from_bytes,
    RSAPublicKey,
    RSAPrivateKey,
)
from ferdelance_shared.decode import decode_from_transfer, decrypt, decrypt_stream, decrypt_stream_file, HybridDecrypter
from ferdelance_shared.encode import encode_to_transfer, encrypt, encrypt_stream, encrypt_stream_file, HybridEncrypter

from ..database import SessionLocal, crud
from ..database.settings import KeyValueStore
from ..database.tables import Client, ClientToken
from .schemas.client import ClientJoinRequest
from .config import FILE_CHUNK_SIZE


import json
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
    """Generates a client token with the data received from the client.

    :param client:
        Data received from the outside.
    """

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


def check_token(credentials: HTTPBasicCredentials = Depends(HTTPBearer())) -> str:
    """Check if the given token exists in the database.

    :param db:
        Current session to the database.
    :param token:
        Header token received from the client.
    :return:
        True if the token is valid, otherwise false.
    """
    token = credentials.credentials

    with SessionLocal() as db:
        client_token: ClientToken = crud.get_client_token_by_token(db, token)

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


def get_server_public_key(db: Session) -> RSAPublicKey:
    """
    :param db:
        Current session to the database.
    :return:
        The server public key in string format.
    """
    kvs = KeyValueStore(db)
    public_bytes: bytes = kvs.get_bytes(PUBLIC_KEY)
    return public_key_from_bytes(public_bytes)


def get_server_public_key_str(db: Session) -> str:
    kvs = KeyValueStore(db)
    public_str: bytes = kvs.get_str(PUBLIC_KEY)
    return encode_to_transfer(public_str)


def get_server_private_key(db: Session) -> str:
    """
    :param db:
        Current session to the database.
    :return:
        The server public key in string format.
    """
    kvs = KeyValueStore(db)
    private_bytes: bytes = kvs.get_bytes(PRIVATE_KEY)
    return private_key_from_bytes(private_bytes)


def get_client_public_key(client: Client | ClientJoinRequest) -> RSAPublicKey:
    key_bytes: bytes = decode_from_transfer(client.public_key).encode('utf8')
    public_key: RSAPublicKey = public_key_from_bytes(key_bytes)
    return public_key


def server_encrypt(client: Client, content: str) -> str:
    client_public_key: RSAPublicKey = get_client_public_key(client)
    return encrypt(client_public_key, content)


def server_decrypt(db: Session, content: str) -> str:
    server_private_key: RSAPrivateKey = get_server_private_key(db)
    return decrypt(server_private_key, content)


def server_stream_encrypt(client: Client, path: str) -> StreamingResponse:
    client_public_key: RSAPublicKey = get_client_public_key(client)

    return StreamingResponse(
        encrypt_stream_file(path, client_public_key),
        media_type='application/octet-stream'
    )


async def stream_file(file: UploadFile) -> bytes:
    while chunk := file.read(FILE_CHUNK_SIZE):
        yield chunk
    yield


def server_stream_decrypt_to_file(file: UploadFile, path: str, db: Session) -> None:
    private_key: RSAPrivateKey = get_server_private_key(db)

    decrypt_stream_file(stream_file(file), path, private_key)

    # TODO: find a way to add the checksum to the stream


def server_stream_decrypt_to_dictionary(file: UploadFile, db: Session) -> dict:
    private_key: RSAPrivateKey = get_server_private_key(db)

    dec = HybridDecrypter(private_key)

    content: list[str] = []
    content += dec.start()
    while chunk := file.file.read():
        content += dec.update(chunk)
    content += dec.end()

    return json.loads(''.join(content))

    # TODO: find a way to add the checksum to the stream


def server_memory_stream_encrypt(content: str, client: Client) -> bytes:
    public_key = get_client_public_key(client)

    enc = HybridEncrypter(public_key)

    data: list[str] = []
    data += enc.start()
    data += enc.update(content)
    data += enc.end()

    return ''.join(content)
