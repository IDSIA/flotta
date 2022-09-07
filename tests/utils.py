from sqlalchemy import create_engine
from sqlalchemy.orm import close_all_sessions

from fastapi.testclient import TestClient
from base64 import b64encode
from requests import Response

from ferdelance.database import SessionLocal
from ferdelance.database.settings import setup_settings
from ferdelance.database.startup import init_content
from ferdelance.server.api import api
from ferdelance.server.security import generate_keys

from ferdelance_shared.decode import decrypt, decrypt_stream, decode_from_transfer
from ferdelance_shared.encode import encrypt
from ferdelance_shared.generate import (
    bytes_from_public_key,
    bytes_from_private_key,
    private_key_from_bytes,
    public_key_from_str,
    generate_asymmetric_key,
    RSAPrivateKey,
    RSAPublicKey
)

import hashlib
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


def setup_rsa_keys() -> RSAPrivateKey:
    """Creates a pair of RSA keys for encryption. The private key is saved in the folder specified by 
    the environment variable `PATH_PRIVATE_KEY`.

    :return:
        A tuple composed by a RSAPublicKey and a RSAPrivateKey object.
    """
    # rsa keys
    if not os.path.exists(PATH_PRIVATE_KEY):
        private_key: RSAPrivateKey = generate_asymmetric_key()
        with open(PATH_PRIVATE_KEY, 'wb') as f:
            data: bytes = bytes_from_private_key(private_key)
            f.write(data)

    # read keys from disk
    with open(PATH_PRIVATE_KEY, 'rb') as f:
        data: bytes = f.read()
        private_key: RSAPrivateKey = private_key_from_bytes(data)

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

    server_public_key: RSAPublicKey = public_key_from_str(decode_from_transfer(json_data['public_key']))

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
    return json.loads(
        decrypt(private_key, json_data['payload'])
    )


def create_payload(server_public_key: RSAPublicKey, payload: dict) -> dict:
    return {
        'payload': encrypt(server_public_key, json.dumps(payload)),
    }


def stream_content(content):
    for c in content:
        yield c
    yield


def decrypt_stream_response(stream: Response, private_key: RSAPrivateKey) -> bytes:
    checksum = hashlib.sha256()
    content = bytearray()

    for chunk in decrypt_stream(stream_content(stream.iter_content()), private_key):
        checksum.update(chunk)
        content.extend(chunk)

    return bytes(content), checksum.hexdigest()
