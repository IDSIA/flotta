from .utils import (
    setup_test_database,
    teardown_test_database,
)

from fastapi.testclient import TestClient
from ferdelance.server.api import api

import logging

LOGGER = logging.getLogger(__name__)


class TestClientClass:

    def setup_class(self):
        LOGGER.info('setting up')

        setup_test_database()

        LOGGER.info('setup completed')

    def teardown_class(self):
        LOGGER.info('tearing down')

        teardown_test_database()

        LOGGER.info('tear down completed')

    def test_startup_api(self):
        with TestClient(api) as client:
            LOGGER.info('Startup ok')
