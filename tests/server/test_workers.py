from ferdelance.database.tables import (
    Artifact as ArtifactDB,
    Job as JobDB,
    Result as ResultDB,
)
from ferdelance.database.repositories import ProjectRepository, JobRepository
from ferdelance.server.api import api
from ferdelance.jobs import JobManagementService
from ferdelance.schemas.artifacts import Artifact, ArtifactStatus
from ferdelance.schemas.models import Model
from ferdelance.schemas.plans import TrainAll
from ferdelance.schemas.worker import WorkerTask
from ferdelance.shared.exchange import Exchange
from ferdelance.shared.status import JobStatus, ArtifactJobStatus

from tests.utils import setup_worker, connect, TEST_PROJECT_TOKEN

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import logging
import os
import pickle
import pytest
import uuid

LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_worker_artifact_not_found(session: AsyncSession, exchange: Exchange):
    with TestClient(api) as server:
        await setup_worker(session, exchange)

        res = server.get(
            f"/worker/artifact/{uuid.uuid4()}",
            headers=exchange.headers(),
        )

        assert res.status_code == 404


@pytest.mark.asyncio
async def test_worker_endpoints(session: AsyncSession, exchange: Exchange):
    with TestClient(api) as server:
        args = await connect(server, session)
        worker_id = await setup_worker(session, exchange)

        # prepare new artifact
        pr: ProjectRepository = ProjectRepository(session)
        project = await pr.get_by_token(TEST_PROJECT_TOKEN)

        artifact = Artifact(
            project_id=project.id,
            transform=project.data.extract(),
            model=Model(name="model", strategy=""),
            plan=TrainAll("label").build(),
        )

        # test artifact submit from worker
        res = server.post(
            "/worker/artifact",
            headers=exchange.headers(),
            json=artifact.dict(),
        )

        assert res.status_code == 200

        status: ArtifactStatus = ArtifactStatus(**res.json())

        LOGGER.info(f"artifact_id: {status.id}")

        artifact.id = status.id
        assert artifact.id is not None

        assert status.status is not None
        assert JobStatus[status.status] == JobStatus.SCHEDULED

        res = await session.scalars(select(ArtifactDB).where(ArtifactDB.id == artifact.id).limit(1))
        art_db: ArtifactDB = res.one()

        assert art_db is not None
        assert os.path.exists(art_db.path)

        res = await session.scalars(
            select(JobDB).where(
                JobDB.artifact_id == artifact.id,
                JobDB.component_id == args.client_id,
            )
        )
        job: JobDB = res.one()

        assert JobStatus[job.status] == JobStatus.SCHEDULED

        # simulate client work
        jm: JobManagementService = JobManagementService(session)
        jr: JobRepository = JobRepository(session)

        await jm.client_task_start(job.id, args.client_id)
        await jm.client_result_create(job.id, args.client_id)
        await jr.schedule_job(artifact.id, worker_id)

        await jm.ar.update_status(artifact.id, ArtifactJobStatus.AGGREGATING)

        # check status of job completed by the client
        res = await session.scalars(
            select(JobDB).where(
                JobDB.artifact_id == artifact.id,
                JobDB.component_id == args.client_id,
            )
        )
        job: JobDB = res.one()

        assert JobStatus[job.status] == JobStatus.COMPLETED

        # check status of artifact
        res = await session.scalars(
            select(ArtifactDB).where(
                ArtifactDB.id == artifact.id,
            )
        )
        art_db = res.one()

        assert ArtifactJobStatus[art_db.status] == ArtifactJobStatus.AGGREGATING

        # get job scheduled for worker
        res = await session.scalars(
            select(JobDB).where(
                JobDB.artifact_id == artifact.id,
                JobDB.component_id == worker_id,
                JobDB.status == JobStatus.SCHEDULED.name,
            )
        )
        job_worker: JobDB | None = res.one_or_none()

        assert job_worker is not None

        # simulate worker behavior
        res = server.get(
            f"/worker/task/{job_worker.id}",
            headers=exchange.headers(),
        )

        assert res.status_code == 200

        wt: WorkerTask = WorkerTask(**res.json())

        assert wt.job_id == job_worker.id

        assert artifact.id == wt.artifact.id

        assert artifact.model is not None
        assert wt.artifact.model is not None
        assert len(artifact.transform.stages) == len(wt.artifact.transform.stages)
        assert len(artifact.model.name) == len(wt.artifact.model.name)

        post_d = artifact.dict()
        get_d = wt.artifact.dict()

        assert post_d == get_d

        # test worker submit model
        model_path = os.path.join(".", "model.bin")
        model = {"model": "example_model"}

        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        res = server.post(
            f"/worker/result/{job_worker.id}",
            headers=exchange.headers(),
            files={"file": open(model_path, "rb")},
        )

        assert res.status_code == 200

        res = await session.scalars(select(ResultDB).where(ResultDB.component_id == worker_id))
        results: list[ResultDB] = list(res.all())

        assert len(results) == 1

        result_id = results[0].id

        # test worker get model
        res = server.get(
            f"/worker/result/{result_id}",
            headers=exchange.headers(),
        )

        assert res.status_code == 200

        model_get = pickle.loads(res.content)

        assert isinstance(model_get, type(model))
        assert "model" in model_get
        assert model == model_get

        assert os.path.exists(results[0].path)

        # cleanup
        os.remove(art_db.path)
        os.remove(results[0].path)
        os.remove(model_path)


@pytest.mark.asyncio
async def test_worker_access(session: AsyncSession, exchange: Exchange):
    with TestClient(api) as server:
        await setup_worker(session, exchange)

        res = server.get(
            "/client/update",
            headers=exchange.headers(),
        )

        assert res.status_code == 403

        res = server.get(
            "/worker/artifact/none",
            headers=exchange.headers(),
        )

        assert res.status_code == 404  # there is no artifact, and 404 is correct

        res = server.get(
            "/workbench/clients",
            headers=exchange.headers(),
        )

        assert res.status_code == 403
