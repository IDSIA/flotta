from fastapi.testclient import TestClient
from ferdelance.server.api import api

import logging

LOGGER = logging.getLogger(__name__)


class TestClientClass:

    def test_startup_api(self):
        with TestClient(api) as _:
            LOGGER.info('Startup ok')
