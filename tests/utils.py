from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey, generate_private_key
from cryptography.hazmat.primitives.serialization import load_ssh_public_key

from sqlalchemy import create_engine
from sqlalchemy.orm import close_all_sessions

from fastapi.testclient import TestClient
from base64 import b64encode, b64decode

from ferdelance.database import SessionLocal
from ferdelance.database.settings import setup_settings
from ferdelance.database.startup import init_content
from ferdelance.server.api import api
from ferdelance.server.security import generate_keys, decrypt

import random
import json
import logging
import uuid
import os


LOGGER = logging.getLogger(__name__)

os.environ['SERVER_MAIN_PASSWORD'] = '7386ee647d14852db417a0eacb46c0499909aee90671395cb5e7a2f861f68ca1'

DB_ID = str(uuid.uuid4()).replace('-', '')
DB_HOST = os.environ.get('DB_HOST', 'postgres')
DB_USER = os.environ.get('DB_USER', 'admin')
DB_PASS = os.environ.get('DB_PASS', 'admin')
DB_NAME = os.environ.get('DB_SCHEMA', f'test_{DB_ID}')

PATH_PRIVATE_KEY = os.environ.get('PATH_PRIVATE_KEY', str(os.path.join('tests', 'private_key.pem')))


def decrypt(pk: RSAPrivateKey, text: str) -> str:
    """Local utility to decrypt a text using a private key.
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


def encrypt(public_key: RSAPublicKey, text: str) -> str:
    """Local utility to encrypt a text using a public key.

    :param public_key:
        Server public key in bytes.
    :param text:
        Content to be encrypted.
    """
    plain_text: bytes = text.encode('utf8')
    enc_text: bytes = public_key.encrypt(plain_text, padding.PKCS1v15())
    b64_text: bytes = b64encode(enc_text)
    ret_text: str = b64_text.decode('utf8')
    return ret_text


def setup_test_client() -> TestClient:
    """Creates FastAPI mockup for test the server."""
    return TestClient(api)


def setup_test_database() -> tuple[str, str]:
    """Creates a new database on the remote server specified by `DB_HOST`, `DB_USER`, and `DB_PASS` (all env variables.).
    The name of the database is randomly generated using UUID4, if not supplied via `DB_SCHEMA` env variable.
    The database will be used as the server's database.

    :return:
        A tuple composed by a connection string to the database and a second connection string to a default database (used for the teardown).
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


def bytes_from_public_key(private_key: RSAPublicKey) -> bytes:
    return private_key.public_bytes(
        encoding=serialization.Encoding.OpenSSH,
        format=serialization.PublicFormat.OpenSSH,
    )


def public_key_from_str(public_key: str) -> RSAPublicKey:
    return load_ssh_public_key(public_key.encode('utf8'), default_backend())


def setup_rsa_keys() -> RSAPrivateKey:
    """Creates a pair of RSA keys for encryption. The private key is saved in the folder specified by 
    the environment variable `PATH_PRIVATE_KEY`.

    :return:
        A tuple composed by a RSAPublicKey and a RSAPrivateKey object.
    """
    # rsa keys
    if not os.path.exists(PATH_PRIVATE_KEY):
        private_key: RSAPrivateKey = generate_private_key(
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
        private_key: RSAPrivateKey = serialization.load_pem_private_key(
            data,
            None,
            backend=default_backend())

    LOGGER.info('RSA keys created')

    return private_key


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


def create_client(client: TestClient, private_key: RSAPrivateKey) -> tuple[str, str, RSAPublicKey]:
    """Creates and register a new client with random mac_address and node.
    :return:
        Client id and token for this new client.
    """
    mac_address = "02:00:00:%02x:%02x:%02x" % (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
    node = 1000000000000 + int(random.uniform(0, 1.0) * 1000000000)

    public_key: bytes = bytes_from_public_key(private_key.public_key())
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

    LOGGER.info(f'client_id={client_id}: sucessfully created new client')

    server_public_key: RSAPublicKey = public_key_from_str(json_data['public_key'])

    return client_id, client_token, server_public_key


def headers(token) -> dict[str, str]:
    """Build a dictionary with the headers required by the server.
    :param token:
        Connection token generated by the server.
    """
    return {
        'Authorization': f'Bearer {token}'
    }


def get_payload(private_key: RSAPrivateKey, json_data: dict) -> dict:
    return json.loads(decrypt(private_key, json_data['payload']))


def create_payload(server_public_key: RSAPublicKey, payload: dict) -> dict:
    return {
        'payload': encrypt(server_public_key, json.dumps(payload)),
    }
