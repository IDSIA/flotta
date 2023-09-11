from ferdelance.config import config_manager
from ferdelance.logging import get_logger
from ferdelance.database.repositories import ComponentRepository, ResultRepository, JobRepository, ArtifactRepository
from ferdelance.node.api import api
from ferdelance.workbench.interface import (
    AggregatedDataSource,
    Project,
    Artifact,
    ArtifactStatus,
)
from ferdelance.schemas.models import Model
from ferdelance.schemas.queries import Query
from ferdelance.schemas.workbench import (
    WorkbenchClientList,
    WorkbenchDataSourceIdList,
    WorkbenchProjectToken,
    WorkbenchArtifact,
)
from ferdelance.shared.status import ArtifactJobStatus

from tests.utils import connect, TEST_PROJECT_TOKEN

from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

import json
import os
import pytest
import shutil

LOGGER = get_logger(__name__)


@pytest.mark.asyncio
async def test_workbench_connect(session: AsyncSession):
    with TestClient(api) as server:
        args = await connect(server, session)
        client_id = args.client_id
        wb_id = args.wb_id

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

        headers, _ = wb_exc.create(args.wb_id, set_encryption=False)

        res = server.get(
            "/workbench",
            headers=headers,
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

        headers, payload = wb_exc.create(args.wb_id, wpt.json())

        res = server.request(
            method="GET",
            url="/workbench/project",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 200

        _, res_payload = wb_exc.get_payload(res.content)

        project = Project(**json.loads(res_payload))

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

        headers, payload = wb_exc.create(args.wb_id, wpt.json())

        res = server.request(
            method="GET",
            url="/workbench/clients",
            headers=headers,
            content=payload,
        )

        res.raise_for_status()

        _, res_payload = wb_exc.get_payload(res.content)

        wcl = WorkbenchClientList(**json.loads(res_payload))
        client_list = wcl.clients

        assert len(client_list) == 1


@pytest.mark.asyncio
async def test_workbench_list_datasources(session: AsyncSession):
    with TestClient(api) as server:
        args = await connect(server, session)
        wb_exc = args.wb_exc

        wpt = WorkbenchProjectToken(token=TEST_PROJECT_TOKEN)

        headers, payload = wb_exc.create(args.wb_id, wpt.json())

        res = server.request(
            method="GET",
            url="/workbench/datasources",
            headers=headers,
            content=payload,
        )

        res.raise_for_status()

        _, res_payload = wb_exc.get_payload(res.content)

        wcl = WorkbenchDataSourceIdList(**json.loads(res_payload))

        assert len(wcl.datasources) == 1


@pytest.mark.asyncio
async def test_workflow_submit(session: AsyncSession):
    with TestClient(api) as server:
        args = await connect(server, session)
        wb_exc = args.wb_exc

        wpt = WorkbenchProjectToken(token=TEST_PROJECT_TOKEN)

        headers, payload = wb_exc.create(args.wb_id, wpt.json())

        res = server.request(
            method="GET",
            url="/workbench/project",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 200

        _, res_payload = wb_exc.get_payload(res.content)

        project = Project(**json.loads(res_payload))

        datasource: AggregatedDataSource = project.data

        assert len(datasource.features) == 2
        assert datasource.n_records == 1000
        assert datasource.n_features == 2

        assert len(datasource.features) == datasource.n_features

        dtypes = [f.dtype for f in datasource.features]

        assert "float" in dtypes
        assert "int" in dtypes

        artifact = Artifact(
            project_id=project.id,
            transform=datasource.extract(),
            model=Model(name="model", strategy=""),
            plan=None,
        )

        headers, payload = wb_exc.create(args.wb_id, artifact.json())

        res = server.post(
            "/workbench/artifact/submit",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 200

        _, res_payload = wb_exc.get_payload(res.content)

        status = ArtifactStatus(**json.loads(res_payload))

        artifact_id = status.id

        assert status.status is not None
        assert artifact_id is not None
        assert ArtifactJobStatus[status.status] == ArtifactJobStatus.SCHEDULED

        wba = WorkbenchArtifact(artifact_id=artifact_id)

        headers, payload = wb_exc.create(args.wb_id, wba.json())

        res = server.request(
            method="GET",
            url="/workbench/artifact/status",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 200

        _, res_payload = wb_exc.get_payload(res.content)

        status = ArtifactStatus(**json.loads(res_payload))

        assert status.status is not None
        assert ArtifactJobStatus[status.status] == ArtifactJobStatus.SCHEDULED

        headers, payload = wb_exc.create(args.wb_id, wba.json())

        res = server.request(
            method="GET",
            url="/workbench/artifact",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 200

        _, res_payload = wb_exc.get_payload(res.content)

        downloaded_artifact = Artifact(**json.loads(res_payload))

        assert downloaded_artifact.id is not None
        assert len(downloaded_artifact.transform.stages) == 1
        assert len(downloaded_artifact.transform.stages[0].features) == 2

        shutil.rmtree(config_manager.get().storage_artifact(artifact_id))


@pytest.mark.asyncio
async def test_get_results(session: AsyncSession):
    with TestClient(api) as server:
        args = await connect(server, session)
        wb_exc = args.wb_exc

        cr: ComponentRepository = ComponentRepository(session)
        ar: ArtifactRepository = ArtifactRepository(session)
        jr: JobRepository = JobRepository(session)
        rr: ResultRepository = ResultRepository(session)

        self_component = await cr.get_self_component()

        artifact = await ar.create_artifact(Artifact(project_id=TEST_PROJECT_TOKEN, transform=Query()))
        await ar.update_status(artifact.id, ArtifactJobStatus.COMPLETED)

        job = await jr.schedule_job(artifact.id, self_component.id, is_aggregation=True, iteration=artifact.iteration)
        await jr.start_execution(job)
        await jr.mark_completed(job.id, self_component.id)

        content = '{"message": "results!"}'
        result = await rr.create_result(job.id, artifact.id, self_component.id, artifact.iteration, is_aggregation=True)
        os.makedirs(os.path.dirname(result.path), exist_ok=True)
        with open(result.path, "w") as f:
            f.write(content)

        wba = WorkbenchArtifact(artifact_id=result.artifact_id)

        headers, payload = wb_exc.create(args.wb_id, wba.json())

        res = server.request(
            "GET",
            "/workbench/result",
            headers=headers,
            content=payload,
        )

        res.raise_for_status()

        assert res.status_code == 200

        _, res_data = wb_exc.get_payload(res.content)

        data = json.loads(res_data)

        assert "message" in data
        assert data["message"] == "results!"


@pytest.mark.asyncio
async def test_workbench_access(session):
    with TestClient(api) as server:
        args = await connect(server, session)
        wb_exc = args.wb_exc

        project_token = args.project_token
        wpt = WorkbenchProjectToken(token=project_token)

        headers, payload = wb_exc.create(args.wb_id, wpt.json())

        res = server.get(
            "/client/update",
            headers=headers,
        )

        assert res.status_code == 403

        res = server.get(
            "/task/result/none",
            headers=headers,
        )

        assert res.status_code == 403

        res = server.request(
            method="GET",
            url="/workbench/clients",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 200
