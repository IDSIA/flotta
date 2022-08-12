from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_ssh_public_key

from hashlib import sha256

from sqlalchemy.orm import Session

from time import time

from database.settings import KeyValueStore

import logging
import os

from .schemas.client import ClientJoinRequest


MAIN_KEY = 'SERVER_MAIN_PASSWORD'
PUBLIC_KEY = 'SERVER_KEY_PUBLIC'
PRIVATE_KEY = 'SERVER_KEY_PRIVATE'


def generate_keys(db: Session) -> None:

    SMP_VALUE = os.environ.get(MAIN_KEY, None)

    if SMP_VALUE is None:
        logging.fatal(f'Environment variable {MAIN_KEY} is missing.')
        raise ValueError(f'{MAIN_KEY} missing')

    kvs = KeyValueStore(db)

    try:
        db_smp_key = kvs.get_str(MAIN_KEY)

        if db_smp_key != SMP_VALUE:
            logging.fatal(f'Environment variable {MAIN_KEY} invalid: please set the correct password!')
            raise Exception(f'{MAIN_KEY} invalid')

    except ValueError:
        kvs.put_str(MAIN_KEY, SMP_VALUE)
        logging.info(f'Application initialization, Environment variable {MAIN_KEY} saved in storage')

    try:
        private_key = kvs.get_bytes(PRIVATE_KEY)
        logging.info('Keys are already available')
        return 

    except ValueError:
        pass

    # generate new keys
    logging.info('Keys generation started')

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

    logging.info('Keys generation completed')


def generate_token(client: ClientJoinRequest, client_uuid: str) -> str:
    ms = round(time() * 1000)

    token = f'{client_uuid}~{client.system}${client.mac_address}£{client.node}={ms};'
    token = sha256().hexdigest().encode('ascii')
    token = sha256(token).hexdigest().encode('ascii')

    return token


def get_server_public_key(db: Session) -> str:
    """
    :param db:
        Current session to the database.
    :return:
        The server public key in string format.
    """
    kvs = KeyValueStore(db)
    return kvs.get_str(PUBLIC_KEY)


def encrypt(public_key_str: str, text: str) -> str:
    public_key: bytes = public_key_str.encode('ascii')
    plain_text: bytes = text.encode('ascii')

    pk: rsa.RSAPublicKey = load_ssh_public_key(public_key, backend=default_backend())

    return pk.encrypt(plain_text, padding.PKCS1v15).decode('ascii')


def decrypt(db: Session, text: str) -> str:
    kvs = KeyValueStore(db)
    private_key = kvs.get_bytes(PRIVATE_KEY)
    plain_text: bytes = text.encode('ascii')

    pk: rsa.RSAPrivateKey = load_pem_private_key(private_key, backend=default_backend())

    return pk.decrypt(text, padding.PKCS1v15).decode('ascii')
