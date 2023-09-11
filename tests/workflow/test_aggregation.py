from typing import Any

from ferdelance.config import DataSourceConfiguration
from ferdelance.client.state import DataSourceStorage
from ferdelance.database import AsyncSession
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.models import (
    GenericModel,
    FederatedRandomForestClassifier,
    StrategyRandomForestClassifier,
    ParametersRandomForestClassifier,
)
from ferdelance.schemas.plans import TrainTestSplit
from ferdelance.schemas.tasks import TaskArguments
from ferdelance.schemas.updates import UpdateData

from tests.serverless import ServerlessExecution
from tests.utils import TEST_PROJECT_TOKEN

import os
import pickle
import pytest


def aggregation_job(args: TaskArguments) -> None:
    print(f"artifact_id={args.artifact_id}: aggregation start for job_id={args.job_id}")


@pytest.mark.asyncio
async def test_aggregation(session: AsyncSession):
    DATA_PATH_1 = os.path.join("tests", "integration", "data", "california_housing.MedInc1.csv")
    DATA_PATH_2 = os.path.join("tests", "integration", "data", "california_housing.MedInc2.csv")

    assert os.path.exists(DATA_PATH_1)
    assert os.path.exists(DATA_PATH_2)

    data1 = DataSourceStorage(
        [
            DataSourceConfiguration(
                name="california1",
                token=[TEST_PROJECT_TOKEN],
                kind="file",
                type="csv",
                path=DATA_PATH_1,
            )
        ],
    )
    data2 = DataSourceStorage(
        [
            DataSourceConfiguration(
                name="california2",
                kind="file",
                type="csv",
                path=DATA_PATH_2,
                token=[TEST_PROJECT_TOKEN],
            )
        ],
    )

    server = ServerlessExecution(session)
    await server.setup()

    await server.create_project(TEST_PROJECT_TOKEN)

    client1 = await server.add_client(1, data1)
    client2 = await server.add_client(2, data2)

    clients = [c.id for c in await server.cr.list_clients()]

    assert len(clients) == 2
    assert client1.client.id in clients
    assert client2.client.id in clients

    # submit artifact

    project = await server.get_project(TEST_PROJECT_TOKEN)

    artifact: Artifact = Artifact(
        project_id=project.id,
        transform=project.extract(),
        plan=TrainTestSplit("MedHouseValDiscrete", 0.2).build(),
        model=FederatedRandomForestClassifier(
            strategy=StrategyRandomForestClassifier.MERGE,
            parameters=ParametersRandomForestClassifier(n_estimators=10),
        ).build(),
    )

    await server.submit(artifact)

    # client 1
    result1 = await client1.next_get_execute_post()

    can_aggregate = await server.check_aggregation(result1)

    assert not can_aggregate

    # client 2
    result2 = await client2.next_get_execute_post()

    can_aggregate = await server.check_aggregation(result2)

    assert can_aggregate

    # aggregate

    job_id = await server.aggregate(result2, aggregation_job)

    # worker
    worker_task = await server.get_worker_task(job_id)

    artifact = worker_task.artifact

    assert artifact.is_model()

    base: Any = None

    agg: GenericModel = artifact.get_model()
    strategy: str = artifact.get_strategy()

    for result_id in worker_task.content_ids:
        result = await server.worker_service.get_result(result_id)

        with open(result.path, "rb") as f:
            partial: GenericModel = pickle.load(f)

        if base is None:
            base = partial
        else:
            base = agg.aggregate(strategy, base, partial)

    await server.post_worker_result(job_id)

    jobs = await server.jr.list_jobs_by_artifact_id(artifact.id)

    assert len(jobs) == 3

    # check
    next_action = await client1.next_action()

    assert isinstance(next_action, UpdateData)

    next_action = await client2.next_action()

    assert isinstance(next_action, UpdateData)
