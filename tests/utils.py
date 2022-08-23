from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding

from sqlalchemy import create_engine
from sqlalchemy.orm import close_all_sessions

from fastapi.testclient import TestClient
from base64 import b64encode, b64decode

from ferdelance.database import SessionLocal
from ferdelance.database.settings import setup_settings
from ferdelance.database.startup import init_content
from ferdelance.server.api import api
from ferdelance.server.security import generate_keys, decrypt

from .utils import decrypt

import json
import random
import logging
import uuid
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)5s %(message)s',
)

LOGGER = logging.getLogger(__name__)

os.environ['SERVER_MAIN_PASSWORD'] = '7386ee647d14852db417a0eacb46c0499909aee90671395cb5e7a2f861f68ca1'

DB_ID = str(uuid.uuid4()).replace('-', '')
DB_HOST = os.environ.get('DB_HOST', 'postgres')
DB_USER = os.environ.get('DB_USER', 'admin')
DB_PASS = os.environ.get('DB_PASS', 'admin')
DB_NAME = os.environ.get('DB_SCHEMA', f'test_{DB_ID}')

PATH_PRIVATE_KEY = os.environ.get('PATH_PRIVATE_KEY', str(os.path.join('tests', 'private_key.pem')))


def decrypt(pk: rsa.RSAPrivateKey, text: str) -> str:
    """Local utility to decript a text using a private key.
    Consider that the text should be a string encoded in UTF-8 and base64.
    :param pk:
        Private key to use.
    :param text:
        Encripted text to decrypt.
    :return:
        The decrypted text in UTF-8 encoding.
    """
    b64_text: bytes = text.encode('utf8')
    enc_text: bytes = b64decode(b64_text)
    plain_text: bytes = pk.decrypt(enc_text, padding.PKCS1v15())
    ret_text: str = plain_text.decode('utf8')
    return ret_text


def setup_test_client() -> TestClient:
    """Creates FastAPI mockup for test the server."""
    return TestClient(api)


def setup_test_database() -> tuple[str, str]:
    """Creates a new database on the remote server specified by `DB_HOST`, `DB_USER`, and `DB_PASS` (all env variables.).
    The name of the database is randomly generated using UUID4, if not supplied via `DB_SCHEMA` env variable.
    The database will be used as the server's database.
    """
    # database
    db_string_no_db = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/postgres'

    with create_engine(db_string_no_db, isolation_level='AUTOCOMMIT').connect() as db:
        db.execute(f'CREATE DATABASE {DB_NAME}')
        db.execute(f'GRANT ALL PRIVILEGES ON DATABASE {DB_NAME} to {DB_USER};')

    db_string = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}'
    os.environ['DATABASE_URL'] = db_string

    # populate database
    with SessionLocal() as db:
        init_content(db)
        generate_keys(db)
        setup_settings(db)

    LOGGER.info(f'created test database {DB_NAME}')

    return db_string, db_string_no_db


def setup_rsa_keys() -> tuple[bytes, rsa.RSAPrivateKey]:
    """Creates a pair of RSA keys for encryption. The private key is saved in the folder specified by 
    the environment variable `PATH_PRIVATE_KEY`.
    """
    # rsa keys
    if not os.path.exists(PATH_PRIVATE_KEY):
        private_key: rsa.RSAPrivateKey = rsa.generate_private_key(
            public_exponent=65537,
            key_size=4096,
            backend=default_backend(),
        )
        with open(PATH_PRIVATE_KEY, 'wb') as f:
            data: bytes = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            f.write(data)

    # read keys from disk
    with open(PATH_PRIVATE_KEY, 'rb') as f:
        data: bytes = f.read()
        private_key: rsa.RSAPrivateKey = serialization.load_pem_private_key(
            data,
            None,
            backend=default_backend())

    public_key: bytes = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.OpenSSH,
        format=serialization.PublicFormat.OpenSSH,
    )

    LOGGER.info('RSA keys created')

    return public_key, private_key


def teardown_test_database(db_string_no_db: str) -> None:
    """Close all still open connections and delete the database created with the `setup_test_database()` method.
    """
    close_all_sessions()
    LOGGER.info('database sessions closed')

    # database
    with create_engine(db_string_no_db, isolation_level='AUTOCOMMIT').connect() as db:
        db.execute(f"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{DB_NAME}' AND pid <> pg_backend_pid()")
        db.execute(f'DROP DATABASE {DB_NAME}')

    LOGGER.info(f'database {DB_NAME} deleted')


def create_client(client: TestClient, public_key: bytes, private_key: rsa.RSAPrivateKey, ret_server_key: bool = False) -> tuple[str, str, bytes]:
    """Creates and register a new client with random mac_address and node.
    :return:
        Client id and token for this new client.
    """

    mac_address = "02:00:00:%02x:%02x:%02x" % (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
    node = 1000000000000 + int(random.uniform(0, 1.0) * 1000000000)

    data = {
        'system': 'Linux',
        'mac_address': mac_address,
        'node': node,
        'public_key': b64encode(public_key).decode('utf8'),
        'version': 'test'
    }

    response_join = client.post('/client/join', json=data)

    assert response_join.status_code == 200

    json_data = response_join.json()
    client_id = decrypt(private_key, json_data['id'])
    client_token = decrypt(private_key, json_data['token'])

    LOGGER.info(f'sucessfully created new client with client_id={client_id}')

    server_public_key: bytes = json_data['public_key']
    return client_id, client_token, server_public_key


def headers(token) -> dict[str, str]:
    """Build a dictionary with the headers required by the server.
    :param token:
        Connection token generated by the server.
    """
    return {
        'Authorization': f'Bearer {token}'
    }
