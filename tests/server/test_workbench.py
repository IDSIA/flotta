from ferdelance.config import conf
from ferdelance.database.services import ComponentService
from ferdelance.server.api import api
from ferdelance.schemas.queries import (
    Query,
    QueryFeature,
    QueryStage,
)
from ferdelance.workbench.interface import (
    DataSource,
    Project,
    Artifact,
    ArtifactStatus,
    Client,
)
from ferdelance.schemas.models import Model
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.workbench import (
    WorkbenchJoinRequest,
    WorkbenchJoinData,
    WorkbenchClientList,
    WorkbenchDataSourceIdList,
    WorkbenchProjectToken,
)
from ferdelance.shared.exchange import Exchange
from ferdelance.shared.status import ArtifactJobStatus

from tests.utils import (
    create_client,
    create_project,
    get_metadata,
    send_metadata,
    TEST_PROJECT_TOKEN,
)

from fastapi.testclient import TestClient
from requests import Response
from sqlalchemy.ext.asyncio import AsyncSession

import json
import logging
import os
import pytest
import shutil

LOGGER = logging.getLogger(__name__)


def setup_exchange() -> Exchange:
    exc = Exchange()
    exc.generate_key()
    return exc


async def connect(server: TestClient, session: AsyncSession) -> tuple[str, str, Exchange, str]:
    p_token = await create_project(session)

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

    return client_id, wjd.id, wb_exc, p_token


@pytest.mark.asyncio
async def test_workbench_connect(session: AsyncSession):
    with TestClient(api) as server:
        client_id, wb_id, _, _ = await connect(server, session)

        cs: ComponentService = ComponentService(session)

        assert client_id is not None
        assert wb_id is not None

        uid = await cs.get_by_id(wb_id)
        cid = await cs.get_by_id(client_id)

        assert uid is not None
        assert cid is not None


@pytest.mark.asyncio
async def test_workbench_read_home(session: AsyncSession):
    """Generic test to check if the home works."""
    with TestClient(api) as server:
        _, _, wb_exc, _ = await connect(server, session)

        res = server.get(
            "/workbench",
            headers=wb_exc.headers(),
        )

        assert res.status_code == 200
        assert res.content.decode("utf8") == '"Workbench 🔧"'


@pytest.mark.asyncio
async def test_workbench_get_project(session: AsyncSession):
    """Generic test to check if the home works."""

    with TestClient(api) as server:
        _, _, wb_exc, token = await connect(server, session)

        wpt = WorkbenchProjectToken(token=token)

        res = server.get(
            "/workbench/project",
            headers=wb_exc.headers(),
            data=wb_exc.create_payload(wpt.dict()),
        )

        assert res.status_code == 200

        project = Project(**wb_exc.get_payload(res.content))

        assert project.token == token
        assert project.n_datasources == 1
        assert project.data.n_features == 2
        assert project.data.n_datasources == 1
        assert project.data.n_clients == 1


@pytest.mark.asyncio
async def test_workbench_list_client(session: AsyncSession):

    with TestClient(api) as server:
        _, _, wb_exc, _ = await connect(server, session)

        wpt = WorkbenchProjectToken(token=TEST_PROJECT_TOKEN)

        res = server.get(
            "/workbench/clients",
            headers=wb_exc.headers(),
            data=wb_exc.create_payload(wpt.dict()),
        )

        res.raise_for_status()

        wcl = WorkbenchClientList(**wb_exc.get_payload(res.content))
        client_list = wcl.clients

        assert len(client_list) == 1


@pytest.mark.asyncio
async def test_workbench_list_datasources(session: AsyncSession):

    with TestClient(api) as server:
        _, _, wb_exc, _ = await connect(server, session)

        wpt = WorkbenchProjectToken(token=TEST_PROJECT_TOKEN)

        res = server.get(
            "/workbench/datasources",
            headers=wb_exc.headers(),
            data=wb_exc.create_payload(wpt.dict()),
        )

        res.raise_for_status()

        wcl = WorkbenchDataSourceIdList(**wb_exc.get_payload(res.content))

        assert len(wcl.datasources) == 1


@pytest.mark.asyncio
async def test_workflow_submit(session: AsyncSession):
    with TestClient(api) as server:
        _, _, wb_exc, _ = await connect(server, session)

        res = server.get(
            "/workbench/datasource/list",
            headers=wb_exc.headers(),
        )

        assert res.status_code == 200

        wdsl = WorkbenchDataSourceIdList(**wb_exc.get_payload(res.content))
        ds_list = wdsl.datasources

        assert len(ds_list) == 1

        datasource_id = ds_list[0]

        res = server.get(
            f"/workbench/datasource/{datasource_id}",
            headers=wb_exc.headers(),
        )

        assert res.status_code == 200

        datasource: DataSource = DataSource(**wb_exc.get_payload(res.content))

        assert len(datasource.features) == 2
        assert datasource.n_records == 1000
        assert datasource.n_features == 2

        assert len(datasource.features) == datasource.n_features

        dtypes = [f.dtype for f in datasource.features]

        assert "float" in dtypes
        assert "int" in dtypes

        artifact = Artifact(
            artifact_id=None,
            transform=Query(
                stages=[
                    QueryStage(
                        features=[
                            QueryFeature(
                                name=f.name,
                                dtype=f.dtype,
                            )
                            for f in datasource.features
                        ]
                    )
                ],
            ),
            model=Model(name="model", strategy=""),
            load=None,
        )

        res = server.post(
            "/workbench/artifact/submit",
            data=wb_exc.create_payload(artifact.dict()),
            headers=wb_exc.headers(),
        )

        assert res.status_code == 200

        status = ArtifactStatus(**wb_exc.get_payload(res.content))

        artifact_id = status.artifact_id

        assert status.status is not None
        assert artifact_id is not None
        assert ArtifactJobStatus[status.status] == ArtifactJobStatus.SCHEDULED

        res = server.get(
            f"/workbench/artifact/status/{artifact_id}",
            headers=wb_exc.headers(),
        )

        assert res.status_code == 200

        status = ArtifactStatus(**wb_exc.get_payload(res.content))
        assert status.status is not None
        assert ArtifactJobStatus[status.status] == ArtifactJobStatus.SCHEDULED

        res = server.get(
            f"/workbench/artifact/{artifact_id}",
            headers=wb_exc.headers(),
        )

        assert res.status_code == 200

        downloaded_artifact = Artifact(**wb_exc.get_payload(res.content))

        assert downloaded_artifact.artifact_id is not None
        assert len(downloaded_artifact.transform.stages) == 1
        assert len(downloaded_artifact.transform.stages[0].features) == 2

        shutil.rmtree(os.path.join(conf.STORAGE_ARTIFACTS, artifact_id))


@pytest.mark.asyncio
async def test_workbench_access(session):
    with TestClient(api) as server:
        _, _, wb_exc, token = await connect(server, session)

        res = server.get(
            "/client/update",
            headers=wb_exc.headers(),
        )

        assert res.status_code == 403

        res = server.get(
            "/worker/artifact/none",
            headers=wb_exc.headers(),
        )

        assert res.status_code == 403

        wpt = WorkbenchProjectToken(token=token)

        res = server.get(
            "/workbench/clients",
            headers=wb_exc.headers(),
            data=wb_exc.create_payload(wpt.dict()),
        )

        assert res.status_code == 200
