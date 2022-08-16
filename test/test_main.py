from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from sqlalchemy import create_engine

from fastapi.testclient import TestClient

from database import SessionLocal
from database.startup import init_content

from server.api import api

import logging
import pytest
import uuid
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)5s %(message)s',
)

DB_ID = str(uuid.uuid4()).replace('-', '')
DB_HOST = os.environ.get('DATABSE_HOST', 'postgres')
DB_USER = os.environ.get('DATABASE_USER', 'admin')
DB_PASS = os.environ.get('DATABASE_PASS', 'admin')
DB_NAME = os.environ.get('DATABASE_SCHEMA', f'test_{DB_ID}')


class TestClass():

    def setup_class(self):
        print('setting up:')

        # client
        self.client = TestClient(api)

        # database
        self.db_string_no_db = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/postgres'

        with create_engine(self.db_string_no_db, isolation_level='AUTOCOMMIT').connect() as db:
            db.execute(f'CREATE DATABASE {DB_NAME}')
            db.execute(f'GRANT ALL PRIVILEGES ON DATABASE {DB_NAME} to {DB_USER};')

        self.db_string = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}'
        os.environ['DATABASE_URL'] = self.db_string

        with SessionLocal() as db:
            init_content(db)

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

        self.public_key = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.OpenSSH,
            format=serialization.PublicFormat.OpenSSH,
        ).decode('ascii')

        print('setup completed')

    def teardown_class(self):
        print('tearing down:')

        # database
        with create_engine(self.db_string_no_db, isolation_level='AUTOCOMMIT').connect() as conn:
            conn.execute(f'DROP DATABASE {DB_NAME}')

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
            'public_key': self.public_key,
            'version': 'test'
        }

        response = self.client.post('/client/join', json=data)

        print(response.content)

        assert response.status_code == 200

        assert False
