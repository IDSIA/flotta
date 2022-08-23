from base64 import b64encode

from ferdelance.database import SessionLocal, crud
from ferdelance.database.settings import KeyValueStore
from ferdelance.database.tables import Client, ClientEvent, ClientToken
from ferdelance.server.security import PUBLIC_KEY

from .utils import (
    setup_test_client,
    setup_test_database,
    setup_rsa_keys,
    teardown_test_database,
    create_client,
    headers,
    bytes_from_public_key,
    get_payload,
    create_payload,
)

import random
import logging

LOGGER = logging.getLogger(__name__)


class TestClientClass:

    def setup_class(self):
        """Class setup. This will be executed once each test. The setup will:
        - Create the client.
        - Create a new database on the remote server specified by `DB_HOST`, `DB_USER`, and `DB_PASS` (all env variables.).
            The name of the database is randomly generated using UUID4, if not supplied via `DB_SCHEMA` env variable.
            The database will be used as the server's database.
        - Populate this database with the required tables.
        - Generate and save to the database the servers' keys using the hardcoded `SERVER_MAIN_PASSWORD`.
        - Generate the local public/private keys to simulate a client application.
        """
        LOGGER.info('setting up')

        self.client = setup_test_client()

        self.db_string, self.db_string_no_db = setup_test_database()

        self.private_key = setup_rsa_keys()
        self.public_key = self.private_key.public_key()
        self.public_key_bytes = bytes_from_public_key(self.public_key)

        random.seed(42)

        LOGGER.info('setup completed')

    def teardown_class(self):
        """Class teardown. This method will ensure that the database is closed and deleted from the remote dbms.
        Note that all database connections still open will be forced to close by this method.
        """
        LOGGER.info('tearing down')

        teardown_test_database(self.db_string_no_db)

        LOGGER.info('teardown completed')

    def test_read_home(self):
        """Generic test to check if the home works."""
        response = self.client.get('/')

        assert response.status_code == 200
        assert response.content.decode('utf8') == '"Hi! 😀"'

    def test_client_connect_successfull(self):
        """Simulates the arrival of a new client. The client will connect with a set of hardcoded values:
        - operative system
        - mac address
        - node identification number
        - its public key
        - the version of the software in use

        Then the server will answer with:
        - an encrypted token
        - an encrypted client id
        - a public key in str format
        """

        client_id, client_token, server_public_key = create_client(self.client, self.private_key)

        with SessionLocal() as db:
            db_client = crud.get_client_by_id(db, client_id)
            db_token = crud.get_client_token_by_client_id(db, client_id)

            assert db_client is not None
            assert db_client.active is True
            assert db_token is not None
            assert db_token.token == client_token
            assert db_token.valid is True

            kvs = KeyValueStore(db)
            server_public_key_db: str = kvs.get_str(PUBLIC_KEY)

            assert server_public_key_db == bytes_from_public_key(server_public_key).decode('utf8')

            db_events: list[ClientEvent] = crud.get_all_client_events(db, db_client)

            assert len(db_events) == 1
            assert db_events[0].event == 'creation'

    def test_client_already_exists(self):
        """This test will send twice the access information and expect the second time to receive a 403 error."""

        client_id, _, _ = create_client(self.client, self.private_key)
        public_key = bytes_from_public_key(self.public_key)

        with SessionLocal() as db:
            client = crud.get_client_by_id(db, client_id)

            data = {
                'system': client.machine_system,
                'mac_address': client.machine_mac_address,
                'node': client.machine_node,
                'public_key': b64encode(public_key).decode('utf8'),
                'version': 'test'
            }

        response = self.client.post('/client/join', json=data)

        assert response.status_code == 403
        assert response.json()['detail'] == 'Invalid client data'

    def test_client_invalid_data(self):
        """This test will send invalid data to the server."""
        # TODO: what kind of data is invalid? Public key format? MAC address? Version mismatch?
        pass

    def test_client_update(self):
        """This will test the endpoint for updates."""

        client_id, token, server_public_key = create_client(self.client, self.private_key)
        payload = create_payload(server_public_key, {})

        response_update = self.client.get('/client/update', json=payload, headers=headers(token))

        LOGGER.info(f'response_update={response_update.json()}')

        assert response_update.status_code == 200

        payload = get_payload(self.private_key, response_update.json())

        assert payload['action'] == 'nothing'

        with SessionLocal() as db:
            db_client: Client = crud.get_client_by_id(db, client_id)

            assert db_client is not None

            db_events: list[str] = [c.event for c in crud.get_all_client_events(db, db_client)]

            assert len(db_events) == 3
            assert 'creation' in db_events
            assert 'update' in db_events
            assert 'action:nothing' in db_events

    def test_client_leave(self):
        """This will test the endpoint for leave a client."""
        client_id, token, _ = create_client(self.client, self.private_key)

        response_leave = self.client.post('/client/leave', json={}, headers=headers(token))

        LOGGER.info(f'response_leave={response_leave}')

        assert response_leave.status_code == 200

        # cannot get other updates
        response_update = self.client.get('/client/update', json={}, headers=headers(token))

        assert response_update.status_code == 403

        with SessionLocal() as db:
            db_client: Client = crud.get_client_by_id(db, client_id)

            assert db_client is not None
            assert db_client.active is False
            assert db_client.left is True

            db_events: list[str] = [c.event for c in crud.get_all_client_events(db, db_client)]

            assert 'creation' in db_events
            assert 'left' in db_events
            assert 'update' not in db_events

    def test_client_update_token(self):
        """This will test the failure and update of a token."""
        with SessionLocal() as db:
            client_id, token, server_key = create_client(self.client, self.private_key)

            # expire token
            db.query(ClientToken).filter(ClientToken.client_id == client_id).update({'expiration_time': 0})
            db.commit()

            LOGGER.info('expiration_time for token set to 0')

            payload = create_payload(server_key, {})

            response_update = self.client.get('/client/update', json=payload, headers=headers(token))

            assert response_update.status_code == 200

            response_payload = get_payload(self.private_key, response_update.json())

            assert 'action' in response_payload
            assert response_payload['action'] == 'update_token'

            LOGGER.debug(response_payload)

            new_token = response_payload['data']

            # extend expire token
            db.query(ClientToken).filter(ClientToken.client_id == client_id).update({'expiration_time': 86400})
            db.commit()

            LOGGER.info('expiration_time for token set to 24h')

            response_update = self.client.get('/client/update', json=payload, headers=headers(token))

            assert response_update.status_code == 403

            response_update = self.client.get('/client/update', json=payload, headers=headers(new_token))
            response_payload = get_payload(self.private_key, response_update.json())

            assert response_update.status_code == 200
            assert 'action' in response_payload
            assert response_payload['action'] == 'nothing'
