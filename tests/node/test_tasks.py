from flotta.const import TYPE_NODE
from flotta.config import config_manager
from flotta.core.artifacts import Artifact
from flotta.database.repositories.artifact import ArtifactRepository
from flotta.database.tables import Artifact as ArtifactDB, Job as JobDB
from flotta.database.repositories import ProjectRepository, ComponentRepository, AsyncSession, JobRepository
from flotta.logging import get_logger
from flotta.node.api import api
from flotta.node.services import JobManagementService
from flotta.schemas.components import Component
from flotta.security.exchange import Exchange
from flotta.shared.status import JobStatus, ArtifactJobStatus
from flotta.tasks.tasks import TaskRequest

from tests.utils import connect, TEST_PROJECT_TOKEN, create_node
from tests.dummies import DummyModel

from fastapi.testclient import TestClient
from sqlalchemy import select

import os
import pytest
import uuid

LOGGER = get_logger(__name__)


@pytest.mark.asyncio
async def test_task_task_not_found(session: AsyncSession):
    with TestClient(api) as server:
        exchange: Exchange = create_node(server, TYPE_NODE)

        tpr = TaskRequest(
            artifact_id=str(uuid.uuid4()),
            job_id=str(uuid.uuid4()),
        )

        headers, payload = exchange.create(tpr.model_dump_json())

        res = server.request(
            "GET",
            "/task/params",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 404


async def list_jobs_by_artifact_and_component(
    session: AsyncSession,
    artifact_id: str,
    component_id: str,
) -> list[JobDB]:
    res = await session.scalars(
        select(JobDB).where(
            JobDB.artifact_id == artifact_id,
            JobDB.component_id == component_id,
        )
    )
    jobs: list[JobDB] = list(res.all())
    return jobs


async def _count_jobs(session: AsyncSession, artifact_id: str, expected_n_jobs: int) -> list[JobDB]:
    res = await session.scalars(select(JobDB).where(JobDB.artifact_id == artifact_id))
    jobs: list[JobDB] = list(res.all())

    assert len(jobs) == expected_n_jobs

    return jobs


async def _test_for_status(
    session: AsyncSession,
    artifact_id: str,
    component_id: str,
    expected_status: JobStatus,
    expected_n_jobs: int = 1,
    job_to_test: int = 0,
) -> JobDB:
    jobs = await list_jobs_by_artifact_and_component(session, artifact_id, component_id)

    assert len(jobs) == expected_n_jobs
    job = jobs[job_to_test]
    assert JobStatus[job.status] == expected_status

    return job


@pytest.mark.asyncio
async def test_task_endpoints(session: AsyncSession):
    with TestClient(api) as server:
        cr: ComponentRepository = ComponentRepository(session)
        jr: JobRepository = JobRepository(session)
        ar: ArtifactRepository = ArtifactRepository(session)

        args = await connect(server, session)

        # client: he who has the data
        cl_id = args.cl_id

        # server: he who schedule jobs
        sc: Component = await cr.get_self_component()
        sc_id = sc.id

        private_key_path = config_manager.get().private_key_location()
        sc_exc = Exchange(args.cl_id, private_key_path=private_key_path)
        sc_exc.set_remote_key(args.sv_id, sc.public_key)

        # prepare new artifact
        pr: ProjectRepository = ProjectRepository(session)
        project = await pr.get_by_token(TEST_PROJECT_TOKEN)

        model = DummyModel(query=project.extract())

        artifact = Artifact(
            id="",
            project_id=project.id,
            steps=model.get_steps(),
        )

        # fake submit of artifact
        jms: JobManagementService = JobManagementService(session, sc)

        status = await jms.submit_artifact(artifact)

        LOGGER.info(f"artifact_id: {status.id}")

        artifact.id = status.id
        assert artifact.id is not None

        assert status.status is not None
        assert status.status == ArtifactJobStatus.SCHEDULED

        await jms.check(artifact.id)
        status = await ar.get_status(artifact.id)
        assert status.status == ArtifactJobStatus.RUNNING

        res = await session.scalars(select(ArtifactDB).where(ArtifactDB.id == artifact.id).limit(1))
        art_db: ArtifactDB = res.one()

        assert art_db is not None
        assert os.path.exists(art_db.path)

        await _count_jobs(session, artifact.id, 2)

        sc_job = await jr.get_by_artifact(artifact.id, sc_id, 0)
        sc_job_id = sc_job.id

        cl_job = await jr.get_by_artifact(artifact.id, cl_id, 0)
        cl_job_id = cl_job.id

        unlocked_jobs = await jr.list_unlocked_jobs_by_artifact_id(artifact.id)
        locked_jobs = await jr.list_locked_jobs_by_artifact_id(artifact.id)

        assert len(unlocked_jobs) == 1
        assert len(locked_jobs) == 1

        assert unlocked_jobs[0].component_id == cl_id
        assert locked_jobs[0].component_id == sc_id

        await _test_for_status(session, artifact.id, cl_id, JobStatus.SCHEDULED, 1)
        await _test_for_status(session, artifact.id, sc_id, JobStatus.WAITING, 1)

        # simulate client accept task
        next_cl_job = await jms.next_task_for_component(cl_id)

        assert next_cl_job == cl_job_id

        await jms.task_started(cl_job_id)

        await _test_for_status(session, artifact.id, cl_id, JobStatus.RUNNING, 1)

        # simulate client run task
        await _test_for_status(session, artifact.id, sc_id, JobStatus.WAITING, 1)

        await jms.task_completed(cl_job_id)
        await jms.check(artifact.id)

        await _test_for_status(session, artifact.id, cl_id, JobStatus.COMPLETED, 1)
        await _test_for_status(session, artifact.id, sc_id, JobStatus.SCHEDULED, 1)

        # check for unlocks
        unlocked_jobs = await jr.list_unlocked_jobs_by_artifact_id(artifact.id)
        locked_jobs = await jr.list_locked_jobs_by_artifact_id(artifact.id)

        assert len(unlocked_jobs) == 2
        assert len(locked_jobs) == 0

        # check status of artifact
        res = await session.scalars(
            select(ArtifactDB).where(
                ArtifactDB.id == artifact.id,
            )
        )
        art_db = res.one()

        assert ArtifactJobStatus[art_db.status] == ArtifactJobStatus.RUNNING

        # simulate second step
        next_sc_job = await jms.next_task_for_component(sc_id)

        assert next_sc_job == sc_job_id

        await jms.task_started(sc_job_id)

        await _test_for_status(session, artifact.id, sc_id, JobStatus.RUNNING, 1)

        await jms.task_completed(sc_job_id)
        await jms.check(artifact.id)

        await _test_for_status(session, artifact.id, sc_id, JobStatus.COMPLETED, 1)

        # check status of artifact
        res = await session.scalars(
            select(ArtifactDB).where(
                ArtifactDB.id == artifact.id,
            )
        )
        art_db = res.one()

        assert ArtifactJobStatus[art_db.status] == ArtifactJobStatus.COMPLETED


@pytest.mark.asyncio
async def test_task_access(session: AsyncSession):
    with TestClient(api) as server:
        args = await connect(server, session)

        headers, _ = args.cl_exc.create()

        res = server.get(
            "/client/update",
            headers=headers,
        )

        assert res.status_code == 403

        res = server.get(
            "/task/resource/none",  # TODO: change this to something similar
            headers=headers,
        )

        assert res.status_code == 404  # there is no artifact, and 404 is correct

        res = server.get(
            "/workbench/clients",
            headers=headers,
        )

        assert res.status_code == 403
