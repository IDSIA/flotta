from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from sqlalchemy import create_engine
from sqlalchemy.orm import close_all_sessions

from fastapi.testclient import TestClient
from base64 import b64encode, b64decode

from database import SessionLocal
from database.settings import KeyValueStore
from database.startup import init_content
from database. tables import Client

from server.api import api
from server.security import PUBLIC_KEY, generate_keys, decrypt

import logging
import pytest
import uuid
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)5s %(message)s',
)

os.environ['SERVER_MAIN_PASSWORD'] = '7386ee647d14852db417a0eacb46c0499909aee90671395cb5e7a2f861f68ca1'

DB_ID = str(uuid.uuid4()).replace('-', '')
DB_HOST = os.environ.get('DATABSE_HOST', 'postgres')
DB_USER = os.environ.get('DATABASE_USER', 'admin')
DB_PASS = os.environ.get('DATABASE_PASS', 'admin')
DB_NAME = os.environ.get('DATABASE_SCHEMA', f'test_{DB_ID}')


def decrypt(pk: rsa.RSAPrivateKey, text: str) -> str:
    b64_text: bytes = text.encode('utf8')
    enc_text: bytes = b64decode(b64_text)
    plain_text: bytes = pk.decrypt(enc_text, padding.PKCS1v15())
    ret_text: str = plain_text.decode('utf8')
    return ret_text


class TestClass():

    def setup_class(self):
        print('setting up:')

        # client
        self.client = TestClient(api)

        # database
        self.db_string_no_db = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/postgres'

        db = create_engine(self.db_string_no_db, isolation_level='AUTOCOMMIT').connect()
        try:
            db.execute(f'CREATE DATABASE {DB_NAME}')
            db.execute(f'GRANT ALL PRIVILEGES ON DATABASE {DB_NAME} to {DB_USER};')
        finally:
            db.close()

        self.db_string = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}'
        os.environ['DATABASE_URL'] = self.db_string

        db = SessionLocal()
        try:
            init_content(db)
            generate_keys(db)
        finally:
            db.close()

        # rsa keys
        PATH_PRIVATE_KEY = 'test/private_key.pem'

        if not os.path.exists(PATH_PRIVATE_KEY):
            self.private_key: rsa.RSAPrivateKey = rsa.generate_private_key(
                public_exponent=65537,
                key_size=4096,
                backend=default_backend(),
            )
            with open(PATH_PRIVATE_KEY, 'wb') as f:
                data: bytes = self.private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
                f.write(data)

        with open(PATH_PRIVATE_KEY, 'rb') as f:
            data: bytes = f.read()
            self.private_key: rsa.RSAPrivateKey = serialization.load_pem_private_key(
                data,
                None,
                backend=default_backend())

        self.public_key: bytes = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.OpenSSH,
            format=serialization.PublicFormat.OpenSSH,
        )

        print('setup completed')

    def teardown_class(self):
        print('tearing down:')

        close_all_sessions()

        # database
        conn = create_engine(self.db_string_no_db, isolation_level='AUTOCOMMIT').connect()
        try:
            conn.execute(f"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{DB_NAME}' AND pid <> pg_backend_pid()")
            conn.execute(f'DROP DATABASE {DB_NAME}')
        finally:
            conn.close()

        print('\nteardown completed\n')

    def test_read_home(self):
        response = self.client.get('/')

        assert response.status_code == 200
        assert response.content.decode('utf-8') == '"Hi! 😀"'

    def test_client_connect_successfull(self):
        data = {
            'system': 'Linux',
            'mac_address': 'BE-32-57-6C-04-E2',
            'node': 2485378023427,
            'public_key': b64encode(self.public_key).decode('utf8'),
            'version': 'test'
        }

        response = self.client.post('/client/join', json=data)

        assert response.status_code == 200

        json_data = response.json()

        client_uuid = decrypt(self.private_key, json_data['uuid'])
        client_token = decrypt(self.private_key, json_data['token'])
        server_public_key: bytes = json_data['public_key']

        db = SessionLocal()
        try:
            db_client: Client = db.query(Client).filter(Client.uuid == client_uuid).first()

            assert db_client is not None
            assert db_client.token == client_token

            kvs = KeyValueStore(db)
            server_public_key_db: str = kvs.get_str(PUBLIC_KEY)

            assert server_public_key_db == server_public_key
        finally:
            db.close()
        