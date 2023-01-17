import logging

from fastapi.testclient import TestClient

from ferdelance.server.api import api

LOGGER = logging.getLogger(__name__)


def test_startup_api():
    with TestClient(api) as _:
        LOGGER.info("Startup ok")
