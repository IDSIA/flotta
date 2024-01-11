from typing import Any

from ferdelance.const import TYPE_CLIENT
from ferdelance.core.interfaces import SchedulerContext
from ferdelance.database.repositories import ProjectRepository, AsyncSession, ArtifactRepository, JobRepository
from ferdelance.logging import get_logger
from ferdelance.schemas.client import ClientUpdate
from ferdelance.schemas.components import Component
from ferdelance.schemas.node import JoinData, NodeJoinRequest, NodePublicKey
from ferdelance.schemas.metadata import Metadata, MetaDataSource, MetaFeature
from ferdelance.schemas.workbench import WorkbenchJoinRequest
from ferdelance.security.checksums import str_checksum
from ferdelance.security.exchange import Exchange
from ferdelance.shared.actions import Action
from ferdelance.shared.status import JobStatus

from fastapi.testclient import TestClient
from pydantic import BaseModel

import json
import uuid


LOGGER = get_logger(__name__)


def setup_exchange() -> Exchange:
    exc = Exchange()
    exc.generate_keys()
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

    LOGGER.info(f"component={client_id}: successfully created new client")

    return cjr.id


TEST_PROJECT_TOKEN: str = "a02a9e2ad5901e39bf53388d19e4be46d3ac7efd1366a961cf54c4a4eeb7faa0"
TEST_DATASOURCE_ID: str = "5751619c-ea8a-4a24-b2cb-35c50124c16a"
TEST_DATASOURCE_HASH: str = "ccdd195b3c5611779987fa62194e2e8d89a04651d29ae50de742941ad953e24a"


def get_metadata(
    project_token: str = TEST_PROJECT_TOKEN,
    datasource_id: str = TEST_DATASOURCE_ID,
    ds_hash: str = TEST_DATASOURCE_HASH,
    scale: float = 1.0,
) -> Metadata:
    return Metadata(
        datasources=[
            MetaDataSource(
                id=datasource_id,
                hash=ds_hash,
                tokens=[project_token],
                n_records=int(1000 * scale),
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

    try:
        await ps.create_project("example", p_token)
    except ValueError:
        # project already exists
        pass

    return p_token


class ConnectionArguments(BaseModel):
    cl_id: str
    wb_id: str
    cl_exc: Exchange
    wb_exc: Exchange
    project_token: str

    class Config:
        arbitrary_types_allowed = True


async def connect(api: TestClient, session: AsyncSession, p_token: str = TEST_PROJECT_TOKEN) -> ConnectionArguments:
    await create_project(session, p_token)

    cl_exc = setup_exchange()
    wb_exc = setup_exchange()

    # this is to have a client
    client_id = create_node(api, cl_exc)

    metadata: Metadata = get_metadata()
    send_metadata(client_id, api, cl_exc, metadata)

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
        cl_id=client_id,
        cl_exc=cl_exc,
        wb_id=wb_id,
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


async def assert_jobs_count(
    ar: ArtifactRepository,
    jr: JobRepository,
    artifact_id: str,
    exp_iteration: int,
    exp_jobs_count: int = 0,
    exp_jobs_waiting: int = 0,
    exp_jobs_scheduled: int = 0,
    exp_jobs_running: int = 0,
    exp_jobs_completed: int = 0,
    exp_jobs_failed: int = 0,
) -> None:
    ar_db = await ar.get_artifact(artifact_id)

    jobs_count = await jr.count_jobs_by_artifact_id(artifact_id)

    job_waiting_count = await jr.count_jobs_by_artifact_status(artifact_id, JobStatus.WAITING)
    job_scheduled_count = await jr.count_jobs_by_artifact_status(artifact_id, JobStatus.SCHEDULED)
    job_running_count = await jr.count_jobs_by_artifact_status(artifact_id, JobStatus.RUNNING)
    job_completed_count = await jr.count_jobs_by_artifact_status(artifact_id, JobStatus.COMPLETED)
    job_failed_count = await jr.count_jobs_by_artifact_status(artifact_id, JobStatus.ERROR)

    print("=" * 32)
    print("iteration:     ", ar_db.iteration, "(", exp_iteration, ")")
    print("jobs count:    ", jobs_count, "(", exp_jobs_count, ")")
    print("jobs waiting:  ", job_waiting_count, "(", exp_jobs_waiting, ")")
    print("jobs scheduled:", job_scheduled_count, "(", exp_jobs_scheduled, ")")
    print("jobs running:  ", job_running_count, "(", exp_jobs_running, ")")
    print("jobs completed:", job_completed_count, "(", exp_jobs_completed, ")")
    print("jobs failed:   ", job_failed_count, "(", exp_jobs_failed, ")")
    print("=" * 32)

    assert ar_db.iteration == exp_iteration
    assert jobs_count == exp_jobs_count
    assert job_waiting_count == exp_jobs_waiting
    assert job_scheduled_count == exp_jobs_scheduled
    assert job_running_count == exp_jobs_running
    assert job_completed_count == exp_jobs_completed
    assert job_failed_count == exp_jobs_failed


def get_scheduler_context(n_workers: int = 2) -> SchedulerContext:
    s = Component(id="S", type_name="NODE", public_key="")

    workers = [Component(id=f"W{w}", type_name="NODE", public_key="") for w in range(n_workers)]

    return SchedulerContext(
        artifact_id="artifact",
        initiator=s,
        workers=workers,
    )
