from flotta.config import config_manager
from flotta.core.distributions.many import Collect
from flotta.core.model_operations.aggregations import Aggregation
from flotta.core.model_operations.train import Train, TrainTest
from flotta.core.operations.core import QueryOperation
from flotta.core.steps import Finalize, Parallel
from flotta.core.transformers.splitters import FederatedSplitter
from flotta.database.tables import Job
from flotta.logging import get_logger
from flotta.node.api import api
from flotta.schemas.updates import UpdateData
from flotta.schemas.workbench import (
    WorkbenchProjectToken,
    WorkbenchArtifact,
)
from flotta.shared.actions import Action
from flotta.shared.status import ArtifactJobStatus
from flotta.tasks.tasks import Task
from flotta.workbench.interface import (
    AggregatedDataSource,
    Project,
    Artifact,
    ArtifactStatus,
)

from tests.dummies import DummyModel
from tests.utils import (
    connect,
    client_update,
    TEST_PROJECT_TOKEN,
)

from fastapi.testclient import TestClient
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

import json
import pytest
import shutil

LOGGER = get_logger(__name__)


@pytest.mark.asyncio
async def test_workflow_wb_submit_client_get(session: AsyncSession):
    with TestClient(api) as server:
        args = await connect(server, session)
        client_id = args.cl_id
        wb_exc = args.wb_exc
        cl_exc = args.cl_exc

        # workbench part
        wpt = WorkbenchProjectToken(token=TEST_PROJECT_TOKEN)

        headers, payload = wb_exc.create(wpt.model_dump_json())

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

        model = DummyModel()
        artifact = Artifact(
            id="",
            project_id=project.id,
            steps=[
                Parallel(
                    TrainTest(
                        query=project.extract().add(
                            FederatedSplitter(
                                random_state=42,
                                test_percentage=0.5,
                                label=datasource.features[0].name,
                            )
                        ),
                        trainer=Train(
                            model=model,
                        ),
                        model=model,
                    ),
                    Collect(),
                ),
                Finalize(
                    Aggregation(
                        model=model,
                    ),
                ),
            ],
        )

        headers, payload = wb_exc.create(artifact.model_dump_json())

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

        headers, payload = wb_exc.create(wba.model_dump_json())

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

        assert status.status == ArtifactJobStatus.RUNNING

        headers, payload = wb_exc.create(wba.model_dump_json())

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
        assert len(downloaded_artifact.steps) == len(artifact.steps)
        step0 = downloaded_artifact.steps[0]
        assert isinstance(step0, Parallel)
        assert isinstance(step0.operation, QueryOperation)
        assert step0.operation.query is not None
        assert len(step0.operation.query.stages) == 2
        assert len(step0.operation.query.stages[0].features) == 2

        # client part
        n = await session.scalar(select(func.count()).select_from(Job))
        assert n == 2

        n = await session.scalar(select(func.count()).select_from(Job).where(Job.component_id == client_id))
        assert n == 1

        res = await session.scalars(select(Job).limit(1))
        job: Job = res.one()

        LOGGER.info("update client")

        status_code, action, data = client_update(server, cl_exc)

        assert status_code == 200
        assert Action[action] == Action.EXECUTE

        update_execute = UpdateData(**data)

        assert update_execute.job_id == job.id

        LOGGER.info("get task for client")

        headers, payload = cl_exc.create(update_execute.model_dump_json())

        task_response = server.request(
            method="GET",
            url="/task/",
            headers=headers,
            content=payload,
        )
        assert task_response.status_code == 200

        _, res_payload = cl_exc.get_payload(task_response.content)
        task = Task(**json.loads(res_payload))

        assert isinstance(task.step, Parallel)

        assert task.artifact_id == job.artifact_id
        assert task.artifact_id == status.id
        assert task.project_token == project.token
        assert task.job_id == job.id

        # cleanup

        shutil.rmtree(config_manager.get().storage_artifact(artifact_id))
