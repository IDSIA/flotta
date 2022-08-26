from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_ssh_public_key
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from fastapi.security import HTTPBasicCredentials, HTTPBearer
from fastapi import Depends, HTTPException

from base64 import b64encode, b64decode
from hashlib import sha256
from sqlalchemy.orm import Session
from datetime import timedelta, datetime
from time import time

from ..database import SessionLocal, crud
from ..database.settings import KeyValueStore
from ..database.tables import Client, ClientToken
from .schemas.client import ClientJoinRequest

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

    key: rsa.RSAPrivateKey = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096,
        backend=default_backend(),
    )

    private_key: bytes = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    public_key: bytes = key.public_key().public_bytes(
        encoding=serialization.Encoding.OpenSSH,
        format=serialization.PublicFormat.OpenSSH,
    )

    kvs.put_bytes(PRIVATE_KEY, private_key)
    kvs.put_bytes(PUBLIC_KEY, public_key)

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


def get_server_public_key(db: Session) -> str:
    """
    :param db:
        Current session to the database.
    :return:
        The server public key in string format.
    """
    kvs = KeyValueStore(db)
    return kvs.get_str(PUBLIC_KEY)


def get_client_public_key(client: Client | ClientJoinRequest) -> bytes:
    public_key: str = client.public_key
    plain_text: bytes = public_key.encode('utf8')
    b64_text: bytes = b64decode(plain_text)
    return b64_text


def encrypt(public_key: bytes, text: str) -> str:
    """Encrypt a text to be sent outside of the server.

    :param public_key:
        Client public key in bytes format.
    :param text:
        Content to be encrypted.
    """
    pk: rsa.RSAPublicKey = load_ssh_public_key(public_key, backend=default_backend())

    plain_text: bytes = text.encode('utf8')
    enc_text: bytes = pk.encrypt(plain_text, padding.PKCS1v15())
    b64_text: bytes = b64encode(enc_text)
    ret_text: str = b64_text.decode('utf8')
    return ret_text


def decrypt(db: Session, text: str) -> str:
    """Decrypt a text received from the outside using the private key
    stored in the server.

    :param db:
        Current session to the database.
    :param text:
        Content to be decrypted.
    """
    kvs = KeyValueStore(db)
    private_key = kvs.get_bytes(PRIVATE_KEY)

    pk: rsa.RSAPrivateKey = load_pem_private_key(private_key, None, backend=default_backend())

    b64_text: bytes = text.encode('utf8')
    enc_text: bytes = b64decode(b64_text)
    plain_text: bytes = pk.decrypt(enc_text, padding.PKCS1v15())
    ret_text: str = plain_text.decode('utf8')
    return ret_text


def generate_symmetric_key(key_size: int = 32, iv_size: int = 16) -> tuple[bytes, bytes, Cipher]:
    """Generates a new random key, initialization vector, and cipher for 
    a symmetric encryption algorithm.

    :param key_size:
        Size of the key to generate.
    :param iv_size:
        Size of the initialization vector.
    :return:
        A tuple composed by the key, the initialization vecotr, and the 
        cipher initialized with AES algorithm in CTR mode.
    """
    key: bytes = os.urandom(key_size)
    iv: bytes = os.urandom(iv_size)
    cipher: Cipher = Cipher(algorithms.AES(key), modes.CTR(iv), backend=default_backend())

    return key, iv, cipher


def stream_and_encrypt_file(path: str, public_key: bytes, CHUNK_SIZE: int = 4096, SEPARATOR: bytes = b'\n') -> bytes:
    """Generator function that streams a file from the given path and encrpyt
    the content using an hybrid-encryption algorithm.

    The streamed file is composed by two parts: the first part contains the 
    symmetric key encrypted with the client asymmetric key; the second part
    contains the file encrypted using the asymmetric key.

    The client is expected to decrypt the first part, obtain the symmetric key
    and start decrypte the content of the file.

    :param path:
        Path on disk of the file to stream.
    :param public_key:
        Client public key.
    :param CHUNK_SIZE:
        Size in bytes of each chunk transmitted to the client.
    :param SEPARATOR:
        Single or sequence of bytes that separates the first part of the stream
        from the second part.
    :return:
        A stream of bytes
    """

    # generate session key for hiybrid encrpytion
    key, iv, cipher = generate_symmetric_key()

    data_str: str = json.dumps({
        'key': b64encode(key).decode('utf8'),
        'iv': b64encode(iv).decode('utf8'),
    })

    # first part: return encrypted session key
    yield encrypt(public_key, data_str)

    # return separator between first and second part
    yield SEPARATOR

    # second part: return encrypted file
    encryptor = cipher.encryptor()

    with open(path, mode='rb') as f:
        while True:
            chunk = f.read(CHUNK_SIZE)

            if not chunk:
                yield encryptor.finalize()
                break

            yield encryptor.update(chunk)
