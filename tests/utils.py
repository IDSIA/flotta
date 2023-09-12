from typing import Any

from ferdelance.logging import get_logger
from ferdelance.const import TYPE_CLIENT
from ferdelance.database.repositories import ProjectRepository, AsyncSession
from ferdelance.schemas.client import ClientUpdate
from ferdelance.schemas.node import JoinData, NodeJoinRequest, NodePublicKey
from ferdelance.schemas.metadata import Metadata, MetaDataSource, MetaFeature
from ferdelance.schemas.workbench import WorkbenchJoinRequest
from ferdelance.shared.actions import Action
from ferdelance.shared.checksums import str_checksum
from ferdelance.shared.exchange import Exchange

from fastapi.testclient import TestClient
from pydantic import BaseModel

import json
import uuid

LOGGER = get_logger(__name__)


def setup_exchange() -> Exchange:
    exc = Exchange()
    exc.generate_key()
    return exc


def create_node(api: TestClient, exc: Exchange, type_name: str = TYPE_CLIENT, client_id: str = "") -> str:
    """Creates and register a new client.
    :return:
        Component id for this new client.
    """

    headers = exc.create_header(False)

    response_key = api.get(
        "/node/key",
        headers=headers,
    )

    response_key.raise_for_status()

    spk = NodePublicKey(**response_key.json())

    exc.set_remote_key(spk.public_key)

    assert exc.remote_key is not None

    if not client_id:
        client_id = str(uuid.uuid4())
    public_key = exc.transfer_public_key()

    data_to_sign = f"{client_id}:{public_key}"

    checksum = str_checksum(data_to_sign)
    signature = exc.sign(data_to_sign)

    cjr = NodeJoinRequest(
        id=client_id,
        name="testing_client",
        type_name=type_name,
        public_key=public_key,
        version="test",
        url="http://localhost/",
        checksum=checksum,
        signature=signature,
    )

    _, payload = exc.create_payload(cjr.json())
    headers = exc.create_header(True)

    response_join = api.post(
        "/node/join",
        headers=headers,
        content=payload,
    )

    response_join.raise_for_status()

    _, payload = exc.get_payload(response_join.content)

    jd = JoinData(**json.loads(payload))

    assert len(jd.nodes) == 1

    LOGGER.info(f"client_id={client_id}: successfully created new client")

    return cjr.id


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
                id=datasource_id,
                hash=ds_hash,
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


def send_metadata(component_id: str, api: TestClient, exc: Exchange, metadata: Metadata) -> None:
    headers, payload = exc.create(component_id, metadata.json())

    upload_response = api.post(
        "/node/metadata",
        headers=headers,
        content=payload,
    )

    upload_response.raise_for_status()


async def create_project(session: AsyncSession, p_token: str = TEST_PROJECT_TOKEN) -> str:
    ps = ProjectRepository(session)

    await ps.create_project("example", p_token)

    return p_token


class ConnectionArguments(BaseModel):
    nd_id: str
    wb_id: str
    nd_exc: Exchange
    wb_exc: Exchange
    project_token: str

    class Config:
        arbitrary_types_allowed = True


async def connect(api: TestClient, session: AsyncSession, p_token: str = TEST_PROJECT_TOKEN) -> ConnectionArguments:
    await create_project(session, p_token)

    nd_exc = setup_exchange()
    wb_exc = setup_exchange()

    # this is to have a client
    client_id = create_node(api, nd_exc)

    metadata: Metadata = get_metadata()
    send_metadata(client_id, api, nd_exc, metadata)

    # this is to connect a new workbench
    headers = wb_exc.create_header(False)

    response_key = api.get(
        "/node/key",
        headers=headers,
    )

    response_key.raise_for_status()

    spk = NodePublicKey(**response_key.json())

    wb_exc.set_remote_key(spk.public_key)

    assert wb_exc.remote_key is not None

    wb_id = str(uuid.uuid4())
    public_key = wb_exc.transfer_public_key()

    data_to_sign = f"{wb_id}:{public_key}"

    checksum = str_checksum(data_to_sign)
    signature = wb_exc.sign(data_to_sign)

    wjr = WorkbenchJoinRequest(
        id=wb_id,
        name="test_workbench",
        public_key=wb_exc.transfer_public_key(),
        version="test",
        checksum=checksum,
        signature=signature,
    )

    _, payload = wb_exc.create_payload(wjr.json())
    headers = wb_exc.create_header(True)

    res_connect = api.post(
        "/workbench/connect",
        headers=headers,
        content=payload,
    )

    res_connect.raise_for_status()

    return ConnectionArguments(
        nd_id=client_id,
        wb_id=wb_id,
        nd_exc=nd_exc,
        wb_exc=wb_exc,
        project_token=p_token,
    )


def client_update(component_id: str, api: TestClient, exchange: Exchange) -> tuple[int, str, Any]:
    update = ClientUpdate(action=Action.DO_NOTHING.name)

    headers, payload = exchange.create(component_id, update.json())

    response = api.request(
        method="GET",
        url="/client/update",
        headers=headers,
        content=payload,
    )

    if response.status_code != 200:
        return response.status_code, "", None

    _, res_payload = exchange.get_payload(response.content)

    response_payload = json.loads(res_payload)

    assert "action" in response_payload

    return response.status_code, response_payload["action"], response_payload
