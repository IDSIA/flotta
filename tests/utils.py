from ferdelance.shared.exchange import Exchange
from ferdelance.shared.artifacts import (
    Metadata,
    MetaDataSource,
    MetaFeature,
)
from ferdelance.shared.schemas import (
    ClientJoinRequest,
    ClientJoinData,
)

from fastapi.testclient import TestClient

from requests import Response

import random
import json
import logging


LOGGER = logging.getLogger(__name__)


def create_client(client: TestClient, exc: Exchange) -> str:
    """Creates and register a new client with random mac_address and node.
    :return:
        Client id and token for this new client.
    """
    mac_address = "02:00:00:%02x:%02x:%02x" % (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
    node = 1000000000000 + int(random.uniform(0, 1.0) * 1000000000)

    cjr = ClientJoinRequest(
        system="Linux",
        mac_address=mac_address,
        node=str(node),
        public_key=exc.transfer_public_key(),
        version="test",
    )

    response_join = client.post("/client/join", data=json.dumps(cjr.dict()))

    assert response_join.status_code == 200

    cjd = ClientJoinData(**exc.get_payload(response_join.content))

    LOGGER.info(f"client_id={cjd.id}: successfully created new client")

    exc.set_remote_key(cjd.public_key)
    exc.set_token(cjd.token)

    assert cjd.id is not None
    assert exc.token is not None
    assert exc.remote_key is not None

    return cjd.id


def get_metadata() -> Metadata:
    return Metadata(
        datasources=[
            MetaDataSource(
                n_records=1000,
                n_features=2,
                name="ds1",
                removed=False,
                features=[
                    MetaFeature(
                        name="feature1",
                        dtype="float",
                        v_mean=0.1,
                        v_std=0.2,
                        v_min=0.3,
                        v_p25=0.4,
                        v_p50=0.5,
                        v_p75=0.6,
                        v_miss=0.7,
                        v_max=0.8,
                    ),
                    MetaFeature(
                        name="label",
                        dtype="int",
                        v_mean=0.8,
                        v_std=0.7,
                        v_min=0.6,
                        v_p25=0.5,
                        v_p50=0.4,
                        v_p75=0.3,
                        v_miss=0.2,
                        v_max=0.1,
                    ),
                ],
            )
        ]
    )


def send_metadata(client: TestClient, exc: Exchange, metadata: Metadata) -> Response:
    upload_response = client.post(
        "/client/update/metadata",
        data=exc.create_payload(metadata.dict()),
        headers=exc.headers(),
    )

    return upload_response
