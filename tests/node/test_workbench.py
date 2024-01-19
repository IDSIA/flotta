from ferdelance.config import config_manager
from ferdelance.core.steps import Finalize, Parallel
from ferdelance.database.repositories.projects import ProjectRepository
from ferdelance.logging import get_logger
from ferdelance.database.repositories import (
    ComponentRepository,
    ResourceRepository,
    JobRepository,
    ArtifactRepository,
)
from ferdelance.node.api import api
from ferdelance.node.services.jobs import JobManagementService
from ferdelance.node.services.resource import ResourceManagementService
from ferdelance.security.algorithms import Algorithm
from ferdelance.workbench.interface import (
    AggregatedDataSource,
    Project,
    Artifact,
    ArtifactStatus,
)
from ferdelance.schemas.workbench import (
    WorkbenchClientList,
    WorkbenchDataSourceIdList,
    WorkbenchProjectToken,
    WorkbenchArtifact,
    WorkbenchResource,
)
from ferdelance.shared.status import ArtifactJobStatus, JobStatus

from tests.utils import connect, TEST_PROJECT_TOKEN
from tests.dummies import DummyModel

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
        client_id = args.cl_id
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

        headers, _ = wb_exc.create(args.wb_id, "", Algorithm.NO_ENCRYPTION)

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

        headers, payload = wb_exc.create(args.wb_id, "", Algorithm.NO_ENCRYPTION, wpt.json())

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

        headers, payload = wb_exc.create(args.wb_id, "", wpt.json())

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

        headers, payload = wb_exc.create(args.wb_id, "", wpt.json())

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

        model = DummyModel(query=datasource.extract())

        artifact = Artifact(
            id="",
            project_id=project.id,
            steps=model.get_steps(),
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
        assert status.status == ArtifactJobStatus.RUNNING

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
        assert status.status == ArtifactJobStatus.RUNNING

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
        assert len(downloaded_artifact.steps) == 2
        assert isinstance(downloaded_artifact.steps[0], Parallel)
        assert isinstance(downloaded_artifact.steps[1], Finalize)

        shutil.rmtree(config_manager.get().storage_artifact(artifact_id))


@pytest.mark.asyncio
async def test_get_results(session: AsyncSession):
    with TestClient(api) as server:
        args = await connect(server, session)
        wb_exc = args.wb_exc

        cr: ComponentRepository = ComponentRepository(session)
        ar: ArtifactRepository = ArtifactRepository(session)
        jr: JobRepository = JobRepository(session)
        rr: ResourceRepository = ResourceRepository(session)
        pr: ProjectRepository = ProjectRepository(session)

        self_component = await cr.get_self_component()

        jms: JobManagementService = JobManagementService(session, self_component)
        rms: ResourceManagementService = ResourceManagementService(session)

        project = await pr.get_by_token(args.project_token)

        model = DummyModel()
        artifact = Artifact(
            id="",
            project_id=project.id,
            steps=model.get_steps(),
        )

        # simulate artifact submission
        status = await jms.submit_artifact(artifact)
        artifact.id = status.id
        await jms.check(artifact.id)

        jobs = await jr.list_jobs_by_artifact_id(artifact.id)

        assert len(jobs) == 2

        job1, job2 = jobs

        assert job1.status == JobStatus.SCHEDULED
        assert job2.status == JobStatus.WAITING

        # simulate job1 execution
        unlocked = await jr.list_unlocked_jobs_by_artifact_id(artifact.id)

        assert len(unlocked) == 1
        assert unlocked[0].id == job1.id

        await jms.get_task_by_job_id(job1.id)

        job1 = await jr.get_by_id(job1.id)
        assert job1.status == JobStatus.RUNNING

        await jms.task_completed(job1.id)
        await jms.check(artifact.id)

        job1 = await jr.get_by_id(job1.id)
        assert job1.status == JobStatus.COMPLETED

        # simulate job2 execution
        unlocked = await jr.list_unlocked_jobs_by_artifact_id(artifact.id)

        assert len(unlocked) == 2
        assert unlocked[1].id == job2.id

        await jms.get_task_by_job_id(job2.id)

        job2 = await jr.get_by_id(job2.id)
        assert job2.status == JobStatus.RUNNING

        res = await rms.store_resource(job2.id, job2.component_id)

        await jms.task_completed(job2.id)
        await jms.check(artifact.id)

        job2 = await jr.get_by_id(job2.id)
        assert job2.status == JobStatus.COMPLETED

        # check artifact status
        status = await ar.get_status(artifact.id)
        assert status.status == ArtifactJobStatus.COMPLETED

        resource = await rr.get_by_job_id(job2.id)
        assert resource.is_ready
        assert not resource.is_error
        assert not resource.is_external

        os.makedirs(os.path.dirname(resource.path), exist_ok=True)
        with open(resource.path, "w") as f:
            f.write('{"message": "results!"}')

        wbr = WorkbenchResource(resource_id=resource.id, producer_id=job2.component_id)

        headers, payload = wb_exc.create(args.wb_id, wbr.json())

        res = server.request(
            "GET",
            "/workbench/resource",
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
            "/task/",
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
