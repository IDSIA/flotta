from ferdelance.const import TYPE_NODE
from ferdelance.config import config_manager
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
from ferdelance.schemas.components import Component
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
async def test_task_task_not_found(session: AsyncSession, exchange: Exchange):
    with TestClient(api) as server:
        node_id = create_node(server, exchange, TYPE_NODE)

        tpr = TaskParametersRequest(
            artifact_id=str(uuid.uuid4()),
            job_id=str(uuid.uuid4()),
        )

        headers, payload = exchange.create(node_id, tpr.json())

        res = server.request(
            "GET",
            "/task/params",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 404


@pytest.mark.asyncio
async def test_task_endpoints(session: AsyncSession):
    with TestClient(api) as server:
        cr: ComponentRepository = ComponentRepository(session)

        args = await connect(server, session)

        # workbench: he who submits artifacts
        wb_id = args.wb_id
        wb = await cr.get_by_id(wb_id)

        # client: he who has the data
        cl_id = args.cl_id
        cl_exc = args.cl_exc
        cl = await cr.get_by_id(cl_id)

        # server: he who aggretates
        sc: Component = await cr.get_self_component()
        sc_id = sc.id
        sc_exc = Exchange(config_manager.get().private_key_location())
        sc_exc.set_remote_key(sc.public_key)

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
        jms: JobManagementService = JobManagementService(session, wb)

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
                JobDB.component_id == args.cl_id,
            )
        )
        jobs: list[JobDB] = list(res.all())

        assert len(jobs) == 1
        job = jobs[0]
        assert JobStatus[job.status] == JobStatus.SCHEDULED

        # simulate client train task
        jm: JobManagementService = JobManagementService(session, cl)

        await jm.task_start(job.id)
        result = await jm.task_completed(job.id)

        def ignore(task: TaskArguments) -> None:
            return

        await jm.check(result.job_id, ignore)

        # check status of job completed by the client
        res = await session.scalars(
            select(JobDB).where(
                JobDB.artifact_id == artifact.id,
                JobDB.component_id == args.cl_id,
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

        # get job scheduled for aggregation task
        res = await session.scalars(
            select(JobDB).where(
                JobDB.artifact_id == artifact.id,
                JobDB.component_id == sc_id,
                JobDB.status == JobStatus.SCHEDULED.name,
            )
        )
        job_aggregate: JobDB | None = res.one_or_none()

        assert job_aggregate is not None

        # simulate aggregation behavior: get task parameters
        tpr = TaskParametersRequest(artifact_id=artifact.id, job_id=job_aggregate.id)

        headers, payload = sc_exc.create(sc_id, tpr.json())

        res = server.request(
            "GET",
            "/task/params",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 200

        _, res_data = sc_exc.get_payload(res.content)

        wt: TaskParameters = TaskParameters(**json.loads(res_data))

        assert wt.job_id == job_aggregate.id

        assert artifact.id == wt.artifact.id

        assert artifact.model is not None
        assert wt.artifact.model is not None
        assert len(artifact.transform.stages) == len(wt.artifact.transform.stages)
        assert len(artifact.model.name) == len(wt.artifact.model.name)

        post_d = artifact.dict()
        get_d = wt.artifact.dict()

        assert post_d == get_d

        # test submit of a (fake) aggregated model
        model_path_in = os.path.join(".", "tests", "storage", "model.bin")
        model_path_out = os.path.join(".", "tests", "storage", "model.enc")
        model = {"model": "example_model"}

        with open(model_path_in, "wb") as f:
            pickle.dump(model, f)

        checksum = sc_exc.encrypt_file_for_remote(model_path_in, model_path_out)
        headers = sc_exc.create_signed_header(sc_id, checksum)

        res = server.post(
            f"/task/result/{job_aggregate.id}",
            headers=headers,
            content=open(model_path_out, "rb"),
        )

        assert res.status_code == 200

        res = await session.scalars(select(ResultDB).where(ResultDB.component_id == sc_id))
        results: list[ResultDB] = list(res.all())

        assert len(results) == 1

        result_id = results[0].id

        # test node get aggregated model
        headers, _ = cl_exc.create(cl_id, "")

        res = server.get(
            f"/task/result/{result_id}",
            headers=headers,
        )

        assert res.status_code == 200

        data, _ = cl_exc.stream_response(res.iter_bytes())

        model_get = pickle.loads(data)

        assert isinstance(model_get, type(model))
        assert "model" in model_get
        assert model == model_get

        assert os.path.exists(results[0].path)

        # cleanup
        os.remove(art_db.path)
        os.remove(results[0].path)
        os.remove(model_path_in)
        os.remove(model_path_out)


@pytest.mark.asyncio
async def test_task_access(session: AsyncSession):
    with TestClient(api) as server:
        args = await connect(server, session)

        nd_id = args.cl_id
        nd_exc = args.cl_exc
        headers, _ = nd_exc.create(nd_id)

        res = server.get(
            "/client/update",
            headers=headers,
        )

        assert res.status_code == 403

        res = server.get(
            "/task/result/none",
            headers=headers,
        )

        assert res.status_code == 404  # there is no artifact, and 404 is correct

        res = server.get(
            "/workbench/clients",
            headers=headers,
        )

        assert res.status_code == 403
