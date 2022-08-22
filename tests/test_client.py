from base64 import b64encode

from ferdelance.database import SessionLocal, crud
from ferdelance.database.settings import KeyValueStore
from ferdelance.database.tables import Client, ClientEvent
from ferdelance.server.security import PUBLIC_KEY, decrypt

from .utils import decrypt, BaseTestClass

import json
import logging

LOGGER = logging.getLogger(__name__)


class TestClientClass(BaseTestClass):

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

        client_id, client_token, server_public_key = self._create_client(True)

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

            assert server_public_key_db == server_public_key

            db_events: list[ClientEvent] = crud.get_all_client_events(db, db_client)

            assert len(db_events) == 1
            assert db_events[0].event == 'creation'

    def test_client_already_exists(self):
        """This test will send twice the access information and expect the second time to receive a 403 error."""

        client_id, _ = self._create_client()

        with SessionLocal() as db:
            client = crud.get_client_by_id(db, client_id)

            data = {
                'system': client.machine_system,
                'mac_address': client.machine_mac_address,
                'node': client.machine_node,
                'public_key': b64encode(self.public_key).decode('utf8'),
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

        client_id, token = self._create_client()

        response_update = self.client.get('/client/update', json={'payload': ''}, headers=self._headers(token))

        LOGGER.info(f'response_update={response_update.json()}')

        assert response_update.status_code == 200

        payload = json.loads(decrypt(self.private_key, response_update.json()['payload']))

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
        client_id, token = self._create_client()

        response_leave = self.client.post('/client/leave', json={}, headers=self._headers(token))

        LOGGER.info(f'response_leave={response_leave}')

        assert response_leave.status_code == 200

        # cannot get other updates
        response_update = self.client.get('/client/update', json={}, headers=self._headers(token))

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
