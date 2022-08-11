from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

from sqlalchemy.orm import Session

from database.settings import KeyValueStore

import logging
import os


def generate_keys(db: Session) -> None:

    SMP_KEY = 'SERVER_MAIN_PASSWORD'
    SMP_VALUE = os.environ.get(SMP_KEY, None)

    PRI_KEY = 'SERVER_KEY_PRIVATE'
    PUB_KEY = 'SERVER_KEY_PUBLIC'

    if SMP_VALUE is None:
        logging.fatal(f'Environment variable {SMP_KEY} is invalid.')
        raise ValueError(f'{SMP_KEY} invalid')

    kvs = KeyValueStore(db)
    private_key = kvs.get_bytes(PRI_KEY)
    
    if private_key is not None:
        logging.info('Keys are already available')
        return 

    # generate new keys
    logging.info('Keys generation started')

    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096,
        backend=default_backend(),
    )

    private_key = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    public_key = key.public_key().public_bytes(
        encoding=serialization.Encoding.OpenSSH,
        format=serialization.PublicFormat.OpenSSH,
    )

    kvs.put_str(SMP_KEY, SMP_VALUE)
    kvs.put_bytes(PRI_KEY, private_key)
    kvs.put_bytes(PUB_KEY, public_key)

    logging.info('Keys generation completed')
