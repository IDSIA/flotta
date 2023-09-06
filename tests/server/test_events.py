from ferdelance.logging import get_logger
from ferdelance.node.api import api

from fastapi.testclient import TestClient

LOGGER = get_logger(__name__)


def test_startup_api():
    with TestClient(api) as _:
        LOGGER.info("Startup ok")
