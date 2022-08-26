from .utils import setup_test_client, setup_test_database, setup_rsa_keys, teardown_test_database, create_client, headers, bytes_from_public_key

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

        self.client = setup_test_client()

        self.db_string, self.db_string_no_db = setup_test_database()
        self.private_key = setup_rsa_keys()
        self.public_key = self.private_key.public_key()
        self.public_key_bytes = bytes_from_public_key(self.public_key)

        self.client_1, self.token_1, self.server_key = create_client(self.client, self.private_key)

        random.seed(42)

        LOGGER.info('setup completed')

    def teardown_class(self):
        LOGGER.info('tearing down')

        teardown_test_database(self.db_string_no_db)

        LOGGER.info('teardown completed')

    def test_workflow_update_client(self):
        LOGGER.info('start workflow')
        LOGGER.info('add new version of the client')

        update_response = self.client.post('/client/update', json={'payload': ''}, headers=headers(self.token_1))

        LOGGER.info(f'{update_response}')

        # assert update_response.status_code == 200

        # TODO

        LOGGER.info('')