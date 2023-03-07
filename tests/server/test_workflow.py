from ferdelance.config import conf
from ferdelance.database.tables import Job
from ferdelance.server.api import api
from ferdelance.workbench.interface import (
    AggregatedDataSource,
    Project,
    Artifact,
    ArtifactStatus,
)
from ferdelance.schemas.client import ClientTask
from ferdelance.schemas.models import Model
from ferdelance.schemas.updates import UpdateExecute
from ferdelance.schemas.plans import TrainTestSplit
from ferdelance.schemas.workbench import (
    WorkbenchProjectToken,
    WorkbenchArtifact,
)
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

import logging
import os
import pytest
import shutil
import logging

LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_workflow_wb_submit_client_get(session: AsyncSession):
    with TestClient(api) as server:
        args = await connect(server, session)
        client_id = args.client_id
        wb_exc = args.wb_exc
        cl_exc = args.cl_exc

        # workbench part

        wpt = WorkbenchProjectToken(token=TEST_PROJECT_TOKEN)

        res = server.get(
            f"/workbench/project",
            headers=wb_exc.headers(),
            data=wb_exc.create_payload(wpt.dict()),
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
            load=TrainTestSplit(
                label=datasource.features[0].name,
                test_percentage=0.5,
            ).build(),
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

        wba = WorkbenchArtifact(artifact_id=artifact_id)

        res = server.get(
            f"/workbench/artifact/status",
            data=wb_exc.create_payload(wba.dict()),
            headers=wb_exc.headers(),
        )

        assert res.status_code == 200

        status = ArtifactStatus(**wb_exc.get_payload(res.content))

        assert status.status is not None
        assert status.artifact_id is not None

        assert ArtifactJobStatus[status.status] == ArtifactJobStatus.SCHEDULED

        res = server.get(
            f"/workbench/artifact",
            data=wb_exc.create_payload(wba.dict()),
            headers=wb_exc.headers(),
        )

        assert res.status_code == 200

        downloaded_artifact = Artifact(**wb_exc.get_payload(res.content))

        assert downloaded_artifact.artifact_id is not None
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

        status_code, action, data = client_update(server, cl_exc)

        assert status_code == 200
        assert Action[action] == Action.EXECUTE_TRAINING

        update_execute = UpdateExecute(**data)

        assert update_execute.artifact_id == job.artifact_id

        LOGGER.info("get task for client")

        with server.get(
            "/client/task",
            data=cl_exc.create_payload(update_execute.dict()),
            headers=cl_exc.headers(),
            stream=True,
        ) as task_response:
            assert task_response.status_code == 200

            content = cl_exc.get_payload(task_response.content)

            task = ClientTask(**content)

            assert TEST_DATASOURCE_HASH in task.datasource_hashes

            art = task.artifact

            assert art.artifact_id == job.artifact_id
            assert art.artifact_id == status.artifact_id
            assert art.project_id == project.project_id
            assert len(art.transform.stages) == 1
            assert len(art.transform.stages[0].features) == 2
            assert art.load is not None
            assert art.load.params["label"] == datasource.features[0].name

        # cleanup

        shutil.rmtree(os.path.join(conf.STORAGE_ARTIFACTS, artifact_id))
