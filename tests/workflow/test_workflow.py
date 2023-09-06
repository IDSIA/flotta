from ferdelance.config import config_manager
from ferdelance.logging import get_logger
from ferdelance.database.tables import Job
from ferdelance.node.api import api
from ferdelance.workbench.interface import (
    AggregatedDataSource,
    Project,
    Artifact,
    ArtifactStatus,
)
from ferdelance.schemas.models import Model
from ferdelance.schemas.updates import UpdateExecute
from ferdelance.schemas.plans import TrainTestSplit
from ferdelance.schemas.workbench import (
    WorkbenchProjectToken,
    WorkbenchArtifact,
)
from ferdelance.schemas.tasks import TaskParameters
from ferdelance.shared.actions import Action
from ferdelance.shared.status import ArtifactJobStatus

from tests.utils import (
    connect,
    client_update,
    TEST_PROJECT_TOKEN,
    TEST_DATASOURCE_HASH,
)

from fastapi.testclient import TestClient
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

import json
import os
import pytest
import shutil

LOGGER = get_logger(__name__)


@pytest.mark.asyncio
async def test_workflow_wb_submit_client_get(session: AsyncSession):
    with TestClient(api) as server:
        args = await connect(server, session)
        client_id = args.client_id
        wb_exc = args.wb_exc
        cl_exc = args.cl_exc

        # workbench part

        wpt = WorkbenchProjectToken(token=TEST_PROJECT_TOKEN)

        headers, payload = wb_exc.create(args.wb_id, wpt.json())

        res = server.request(
            "GET",
            "/workbench/project",
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
            plan=TrainTestSplit(
                label=datasource.features[0].name,
                test_percentage=0.5,
            ).build(),
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
            "GET",
            "/workbench/artifact/status",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 200

        _, res_payload = wb_exc.get_payload(res.content)
        status = ArtifactStatus(**json.loads(res_payload))

        assert status.status is not None
        assert status.id is not None

        assert ArtifactJobStatus[status.status] == ArtifactJobStatus.SCHEDULED

        headers, payload = wb_exc.create(args.wb_id, wba.json())

        res = server.request(
            "GET",
            "/workbench/artifact",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 200

        _, res_payload = wb_exc.get_payload(res.content)
        downloaded_artifact = Artifact(**json.loads(res_payload))

        assert downloaded_artifact.id is not None
        assert len(downloaded_artifact.transform.stages) == 1
        assert len(downloaded_artifact.transform.stages[0].features) == 2

        # client part

        n = await session.scalar(select(func.count()).select_from(Job))
        assert n == 1

        n = await session.scalar(select(func.count()).select_from(Job).where(Job.component_id == client_id))
        assert n == 1

        res = await session.scalars(select(Job).limit(1))
        job: Job = res.one()

        LOGGER.info("update client")

        status_code, action, data = client_update(client_id, server, cl_exc)

        assert status_code == 200
        assert Action[action] == Action.EXECUTE_TRAINING

        update_execute = UpdateExecute(**data)

        assert update_execute.job_id == job.id

        LOGGER.info("get task for client")

        headers, payload = cl_exc.create(args.client_id, update_execute.json())

        task_response = server.request(
            method="GET",
            url="/task/params",
            headers=headers,
            content=payload,
        )
        assert task_response.status_code == 200

        _, res_payload = cl_exc.get_payload(task_response.content)
        task = TaskParameters(**json.loads(res_payload))

        assert TEST_DATASOURCE_HASH in task.content_ids

        art = task.artifact

        assert art.id == job.artifact_id
        assert art.id == status.id
        assert art.project_id == project.id
        assert len(art.transform.stages) == 1
        assert len(art.transform.stages[0].features) == 2
        assert art.plan is not None
        assert art.plan.params["label"] == datasource.features[0].name

        # cleanup

        shutil.rmtree(os.path.join(config_manager.get().storage_artifact(artifact_id)))
