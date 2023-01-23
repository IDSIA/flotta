from ferdelance.server.api import api
from ferdelance.shared.exchange import Exchange

from tests.utils import create_client

from fastapi.testclient import TestClient

import logging

LOGGER = logging.getLogger(__name__)


class TestWorkflowClass:
    def test_workflow_update_client(self, exchange: Exchange):
        LOGGER.info("start workflow")
        LOGGER.info("add new version of the client")

        with TestClient(api) as client:
            create_client(client, exchange)

            update_response = client.post("/client/update", json={"payload": ""}, headers=exchange.headers())

            LOGGER.info(f"{update_response}")

            # assert update_response.status_code == 200

            # TODO
