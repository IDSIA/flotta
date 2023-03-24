from ferdelance.config import conf
from ferdelance.database.repositories import ComponentRepository
from ferdelance.server.api import api
from ferdelance.workbench.interface import (
    AggregatedDataSource,
    Project,
    Artifact,
    ArtifactStatus,
)
from ferdelance.schemas.models import Model
from ferdelance.schemas.workbench import (
    WorkbenchClientList,
    WorkbenchDataSourceIdList,
    WorkbenchProjectToken,
    WorkbenchArtifact,
)
from ferdelance.shared.status import ArtifactJobStatus

from tests.utils import (
    connect,
    TEST_PROJECT_TOKEN,
)

from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

import logging
import os
import pytest
import shutil

LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_workbench_connect(session: AsyncSession):
    with TestClient(api) as server:
        args = await connect(server, session)
        client_id = args.client_id
        wb_id = args.workbench_id

        cr: ComponentRepository = ComponentRepository(session)

        assert client_id is not None
        assert wb_id is not None

        uid = await cr.get_by_id(wb_id)
        cid = await cr.get_by_id(client_id)

        assert uid is not None
        assert cid is not None


@pytest.mark.asyncio
async def test_workbench_read_home(session: AsyncSession):
    """Generic test to check if the home works."""
    with TestClient(api) as server:
        args = await connect(server, session)
        wb_exc = args.wb_exc

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
        args = await connect(server, session)
        wb_exc = args.wb_exc
        token = args.project_token

        wpt = WorkbenchProjectToken(token=token)

        res = server.request(
            method="GET",
            url="/workbench/project",
            headers=wb_exc.headers(),
            content=wb_exc.create_payload(wpt.dict()),
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
        args = await connect(server, session)
        wb_exc = args.wb_exc

        wpt = WorkbenchProjectToken(token=TEST_PROJECT_TOKEN)

        res = server.request(
            method="GET",
            url="/workbench/clients",
            headers=wb_exc.headers(),
            content=wb_exc.create_payload(wpt.dict()),
        )

        res.raise_for_status()

        wcl = WorkbenchClientList(**wb_exc.get_payload(res.content))
        client_list = wcl.clients

        assert len(client_list) == 1


@pytest.mark.asyncio
async def test_workbench_list_datasources(session: AsyncSession):

    with TestClient(api) as server:
        args = await connect(server, session)
        wb_exc = args.wb_exc

        wpt = WorkbenchProjectToken(token=TEST_PROJECT_TOKEN)

        res = server.request(
            method="GET",
            url="/workbench/datasources",
            headers=wb_exc.headers(),
            content=wb_exc.create_payload(wpt.dict()),
        )

        res.raise_for_status()

        wcl = WorkbenchDataSourceIdList(**wb_exc.get_payload(res.content))

        assert len(wcl.datasources) == 1


@pytest.mark.asyncio
async def test_workflow_submit(session: AsyncSession):
    with TestClient(api) as server:
        args = await connect(server, session)
        wb_exc = args.wb_exc

        wpt = WorkbenchProjectToken(token=TEST_PROJECT_TOKEN)

        res = server.request(
            method="GET",
            url="/workbench/project",
            headers=wb_exc.headers(),
            content=wb_exc.create_payload(wpt.dict()),
        )

        assert res.status_code == 200

        project = Project(**wb_exc.get_payload(res.content))

        datasource: AggregatedDataSource = project.data

        assert len(datasource.features) == 2
        assert datasource.n_records == 1000
        assert datasource.n_features == 2

        assert len(datasource.features) == datasource.n_features

        dtypes = [f.dtype for f in datasource.features]

        assert "float" in dtypes
        assert "int" in dtypes

        artifact = Artifact(
            artifact_id=None,
            project_id=project.project_id,
            transform=datasource.extract(),
            model=Model(name="model", strategy=""),
            load=None,
        )

        res = server.post(
            "/workbench/artifact/submit",
            content=wb_exc.create_payload(artifact.dict()),
            headers=wb_exc.headers(),
        )

        assert res.status_code == 200

        status = ArtifactStatus(**wb_exc.get_payload(res.content))

        artifact_id = status.artifact_id

        assert status.status is not None
        assert artifact_id is not None
        assert ArtifactJobStatus[status.status] == ArtifactJobStatus.SCHEDULED

        wba = WorkbenchArtifact(artifact_id=artifact_id)

        res = server.request(
            method="GET",
            url="/workbench/artifact/status",
            content=wb_exc.create_payload(wba.dict()),
            headers=wb_exc.headers(),
        )

        assert res.status_code == 200

        status = ArtifactStatus(**wb_exc.get_payload(res.content))

        assert status.status is not None
        assert ArtifactJobStatus[status.status] == ArtifactJobStatus.SCHEDULED

        res = server.request(
            method="GET",
            url="/workbench/artifact",
            headers=wb_exc.headers(),
            content=wb_exc.create_payload(wba.dict()),
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
        args = await connect(server, session)
        wb_exc = args.wb_exc
        token = args.project_token

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

        res = server.request(
            method="GET",
            url="/workbench/clients",
            headers=wb_exc.headers(),
            content=wb_exc.create_payload(wpt.dict()),
        )

        assert res.status_code == 200
