from ferdelance.database.services import ProjectService, DataSourceService
from ferdelance.schemas.metadata import Metadata, MetaDataSource, MetaFeature
from ferdelance.schemas.client import ClientJoinData, ClientJoinRequest
from ferdelance.shared.exchange import Exchange

from fastapi.testclient import TestClient
from requests import Response
from sqlalchemy.ext.asyncio import AsyncSession

import json
import logging
import random


LOGGER = logging.getLogger(__name__)


def create_client(client: TestClient, exc: Exchange) -> str:
    """Creates and register a new client with random mac_address and node.
    :return:
        Component id and token for this new client.
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
    datasource_id: str = "5751619c-ea8a-4a24-b2cb-35c50124c16a"
    return Metadata(
        datasources=[
            MetaDataSource(
                datasource_id=datasource_id,
                datasource_hash="",
                tokens=[""],
                n_records=1000,
                n_features=2,
                name="ds1",
                removed=False,
                features=[
                    MetaFeature(
                        datasource_hash="",
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
                        datasource_hash="",
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


async def create_project(session: AsyncSession, p_token: str, ds_hash: str) -> tuple[ProjectService, DataSourceService]:
    ps = ProjectService(session)
    ds = DataSourceService(session)

    metadata = Metadata(
        datasources=[
            MetaDataSource(
                name="ds1",
                n_records=10,
                n_features=2,
                datasource_id=None,
                datasource_hash=ds_hash,
                tokens=[p_token],
                features=[
                    MetaFeature(name="feature1", datasource_hash=ds_hash, dtype="object"),
                    MetaFeature(name="feature2", datasource_hash=ds_hash, dtype="object"),
                ],
            )
        ]
    )

    await ps.create("example", p_token)
    await ds.create_or_update_metadata("client1", metadata)
    await ps.add_datasources_from_metadata(metadata)

    return ps, ds
