from ferdelance.const import TYPE_NODE
from ferdelance.logging import get_logger
from ferdelance.database.tables import (
    Artifact as ArtifactDB,
    Job as JobDB,
    Result as ResultDB,
)
from ferdelance.database.repositories import ProjectRepository, ComponentRepository, AsyncSession
from ferdelance.node.api import api
from ferdelance.node.services import JobManagementService
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.models import Model
from ferdelance.schemas.plans import TrainAll
from ferdelance.schemas.tasks import TaskParameters, TaskArguments, TaskParametersRequest
from ferdelance.shared.exchange import Exchange
from ferdelance.shared.status import JobStatus, ArtifactJobStatus

from tests.utils import connect, TEST_PROJECT_TOKEN, create_node

from fastapi.testclient import TestClient
from sqlalchemy import select

import json
import os
import pickle
import pytest
import uuid

LOGGER = get_logger(__name__)


@pytest.mark.asyncio
async def test_worker_task_not_found(exc: Exchange):
    with TestClient(api) as server:
        node_id = create_node(server, exc, TYPE_NODE)

        tpr = TaskParametersRequest(
            artifact_id=str(uuid.uuid4()),
            job_id=str(uuid.uuid4()),
        )

        headers, payload = exc.create(node_id, tpr.json())

        res = server.request(
            "GET",
            "/task/params",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 404


@pytest.mark.asyncio
async def test_worker_endpoints(session: AsyncSession, exc: Exchange):
    with TestClient(api) as server:
        cr: ComponentRepository = ComponentRepository(session)

        component = await cr.get_self_component()

        args = await connect(server, session)

        # prepare new artifact
        pr: ProjectRepository = ProjectRepository(session)
        project = await pr.get_by_token(TEST_PROJECT_TOKEN)

        artifact = Artifact(
            project_id=project.id,
            transform=project.data.extract(),
            model=Model(name="model", strategy=""),
            plan=TrainAll("label").build(),
        )

        # fake submit of artifact
        jms: JobManagementService = JobManagementService(session, component.id)

        status = await jms.submit_artifact(artifact)

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
                JobDB.component_id == args.nd_id,
            )
        )
        job: JobDB = res.one()

        assert JobStatus[job.status] == JobStatus.SCHEDULED

        # simulate client work
        jm: JobManagementService = JobManagementService(session, component.id)

        await jm.client_task_start(job.id, args.nd_id)
        result = await jm.create_result(job.id, args.nd_id)
        await jm.client_task_completed(job.id, args.nd_id)

        def ignore(task: TaskArguments) -> None:
            return

        await jm._start_aggregation(result, ignore)

        # check status of job completed by the client
        res = await session.scalars(
            select(JobDB).where(
                JobDB.artifact_id == artifact.id,
                JobDB.component_id == args.nd_id,
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
                JobDB.component_id == component.id,
                JobDB.status == JobStatus.SCHEDULED.name,
            )
        )
        job_worker: JobDB | None = res.one_or_none()

        assert job_worker is not None

        # simulate worker behavior
        tpr = TaskParametersRequest(artifact_id=artifact.id, job_id=job_worker.id)

        headers, payload = args.nd_exc.create(args.nd_id, tpr.json())

        res = server.request(
            "GET",
            "/task/params",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 200

        _, res_data = args.nd_exc.get_payload(res.content)

        wt: TaskParameters = TaskParameters(**json.loads(res_data))

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
            f"/task/result/{job_worker.id}",
            headers=wk_exc.headers(),
            content=b"".join(wk_exc.stream_from_file(model_path)),
        )

        assert res.status_code == 200

        res = await session.scalars(select(ResultDB).where(ResultDB.component_id == wk_id))
        results: list[ResultDB] = list(res.all())

        assert len(results) == 1

        result_id = results[0].id

        # test worker get model
        res = server.get(
            f"/task/result/{result_id}",
            headers=wk_exc.headers(),
        )

        assert res.status_code == 200

        data, _ = wk_exc.stream_response(res.iter_bytes())

        model_get = pickle.loads(data)

        assert isinstance(model_get, type(model))
        assert "model" in model_get
        assert model == model_get

        assert os.path.exists(results[0].path)

        # cleanup
        os.remove(art_db.path)
        os.remove(results[0].path)
        os.remove(model_path)


@pytest.mark.asyncio
async def test_worker_access(session: AsyncSession):
    with TestClient(api) as server:
        _, wk_exc = await setup_worker(session, server)

        res = server.get(
            "/client/update",
            headers=wk_exc.headers(),
        )

        assert res.status_code == 403

        res = server.get(
            "/task/result/none",
            headers=wk_exc.headers(),
        )

        assert res.status_code == 404  # there is no artifact, and 404 is correct

        res = server.get(
            "/workbench/clients",
            headers=wk_exc.headers(),
        )

        assert res.status_code == 403
