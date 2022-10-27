from ferdelance.server.api import api

from .utils import (
    setup_test_database,
    setup_rsa_keys,
    create_client,
    headers,
    bytes_from_public_key
)
from .crud import (
    delete_client,
    Session,
)

from fastapi.testclient import TestClient

import logging
import random

LOGGER = logging.getLogger(__name__)


class TestWorkflowClass:

    def setup_class(self):
        """Class setup. This will be executed once each test. The setup will:
        - Create two clients.
        - Create a new database on the remote server specified by `DB_HOST`, `DB_USER`, and `DB_PASS` (all env variables.).
            The name of the database is randomly generated using UUID4, if not supplied via `DB_SCHEMA` env variable.
            The database will be used as the server's database.
        - Populate this database with the required tables.
        - Generate and save to the database the servers' keys using the hardcoded `SERVER_MAIN_PASSWORD`.
        - Generate the local public/private keys to simulate a client application.
        """
        LOGGER.info('setting up')

        self.engine = setup_test_database()

        self.private_key = setup_rsa_keys()
        self.public_key = self.private_key.public_key()
        self.public_key_bytes = bytes_from_public_key(self.public_key)

        random.seed(42)

        LOGGER.info('setup completed')

    def test_workflow_update_client(self):
        LOGGER.info('start workflow')
        LOGGER.info('add new version of the client')

        with TestClient(api) as client:
            client_id, token, _ = create_client(client, self.private_key)

            update_response = client.post('/client/update', json={'payload': ''}, headers=headers(token))

            LOGGER.info(f'{update_response}')

            # assert update_response.status_code == 200

            # TODO

            LOGGER.info('')

            with Session(self.engine) as session:
                delete_client(session, client_id)
