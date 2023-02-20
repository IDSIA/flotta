from typing import Any

from ferdelance.database.services import ProjectService, WorkerService
from ferdelance.schemas.metadata import Metadata, MetaDataSource, MetaFeature
from ferdelance.schemas.client import ClientJoinData, ClientJoinRequest, ClientUpdate
from ferdelance.schemas.workbench import WorkbenchJoinData, WorkbenchJoinRequest
from ferdelance.shared.actions import Action
from ferdelance.shared.exchange import Exchange

from fastapi.testclient import TestClient
from pydantic import BaseModel
from requests import Response
from sqlalchemy.ext.asyncio import AsyncSession

import json
import logging
import random


LOGGER = logging.getLogger(__name__)


def setup_exchange() -> Exchange:
    exc = Exchange()
    exc.generate_key()
    return exc


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


async def setup_worker(session: AsyncSession, exchange: Exchange):
    try:
        ws: WorkerService = WorkerService(session)
        worker_token = await ws.get_worker_token()

        exchange.set_token(worker_token)

    except Exception:
        assert False


TEST_PROJECT_TOKEN: str = "a02a9e2ad5901e39bf53388d19e4be46d3ac7efd1366a961cf54c4a4eeb7faa0"
TEST_DATASOURCE_ID: str = "5751619c-ea8a-4a24-b2cb-35c50124c16a"
TEST_DATASOURCE_HASH: str = "ccdd195b3c5611779987fa62194e2e8d89a04651d29ae50de742941ad953e24a"


def get_metadata(
    project_token: str = TEST_PROJECT_TOKEN,
    datasource_id: str = TEST_DATASOURCE_ID,
    ds_hash: str = TEST_DATASOURCE_HASH,
) -> Metadata:
    return Metadata(
        datasources=[
            MetaDataSource(
                datasource_id=datasource_id,
                datasource_hash="",
                tokens=[project_token],
                n_records=1000,
                n_features=2,
                name="ds1",
                removed=False,
                features=[
                    MetaFeature(
                        datasource_hash=ds_hash,
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
                        datasource_hash=ds_hash,
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


async def create_project(session: AsyncSession, p_token: str = TEST_PROJECT_TOKEN) -> str:
    ps = ProjectService(session)

    await ps.create("example", p_token)

    return p_token


class ConnectionArguments(BaseModel):
    client_id: str
    workbench_id: str
    cl_exc: Exchange
    wb_exc: Exchange
    project_token: str

    class Config:
        arbitrary_types_allowed = True


async def connect(server: TestClient, session: AsyncSession, p_token: str = TEST_PROJECT_TOKEN) -> ConnectionArguments:
    await create_project(session, p_token)

    cl_exc = setup_exchange()
    wb_exc = setup_exchange()

    # this is to have a client
    client_id = create_client(server, cl_exc)

    metadata: Metadata = get_metadata()
    upload_response: Response = send_metadata(server, cl_exc, metadata)

    assert upload_response.status_code == 200

    # this is for connect a new workbench
    wjr = WorkbenchJoinRequest(public_key=wb_exc.transfer_public_key())

    res = server.post("/workbench/connect", data=json.dumps(wjr.dict()))

    res.raise_for_status()

    wjd = WorkbenchJoinData(**wb_exc.get_payload(res.content))

    wb_exc.set_remote_key(wjd.public_key)
    wb_exc.set_token(wjd.token)

    return ConnectionArguments(
        client_id=client_id,
        workbench_id=wjd.id,
        cl_exc=cl_exc,
        wb_exc=wb_exc,
        project_token=p_token,
    )


def client_update(client: TestClient, exchange: Exchange) -> tuple[int, str, Any]:
    payload = ClientUpdate(action=Action.DO_NOTHING.name)

    response = client.get("/client/update", data=exchange.create_payload(payload.dict()), headers=exchange.headers())

    if response.status_code != 200:
        return response.status_code, "", None

    response_payload = exchange.get_payload(response.content)

    assert "action" in response_payload

    return response.status_code, response_payload["action"], response_payload
