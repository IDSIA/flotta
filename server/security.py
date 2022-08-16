from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_ssh_public_key

from base64 import b64encode, b64decode
from hashlib import sha256
from sqlalchemy.orm import Session
from time import time

from database.settings import KeyValueStore
from database.tables import Client
from .schemas.client import ClientJoinRequest

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


def generate_token(client: ClientJoinRequest) -> tuple[str, str]:
    """Generates a client token with the data received from the client.

    :param client:
        Data received from the outside.
    """
    client_uuid = str(uuid.uuid4())
    ms = round(time() * 1000)

    token = f'{client_uuid}~{client.system}${client.mac_address}£{client.node}={ms};'
    token = sha256().hexdigest().encode('utf8')
    token = sha256(token).hexdigest()

    return token, client_uuid


def check_token(db: Session, token: str) -> bool:
    """Check if the given token exists in the database.

    :param db:
        Current session to the database.
    :param token:
        Header token received from the client.
    :return:
        True if the token is valid, otherwise false.
    """

    db_client = db.query(Client).filter(Client.token == token).first()

    return db_client is not None


def get_server_public_key(db: Session) -> str:
    """
    :param db:
        Current session to the database.
    :return:
        The server public key in string format.
    """
    kvs = KeyValueStore(db)
    return kvs.get_str(PUBLIC_KEY)


def get_client_public_key(client: Client|ClientJoinRequest) -> bytes:
    public_key: str = client.public_key
    plain_text: bytes = public_key.encode('utf8')
    b64_text: bytes = b64decode(plain_text)
    return b64_text
    

def encrypt(public_key: bytes, text: str) -> str:
    """Encrypt a text to be sent outside of the server.

    :param public_key_str:
        Client public key in string format.
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

    pk: rsa.RSAPrivateKey = load_pem_private_key(private_key, None,backend=default_backend())

    b64_text: bytes = text.encode('utf8')
    enc_text: bytes = b64decode(b64_text)
    plain_text: bytes = pk.decrypt(enc_text, padding.PKCS1v15())
    ret_text: str = plain_text.decode('utf8')
    return ret_text

