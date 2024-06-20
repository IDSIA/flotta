from flotta.config.config import DataSourceConfiguration, DataSourceStorage
from flotta.core.estimators import CountEstimator
from flotta.workbench.interface import Artifact

from tests.utils import TEST_PROJECT_TOKEN, assert_jobs_count
from tests.serverless import ServerlessExecution, ServerlessWorker
from tests.workflow.test_aggregation import load_resource

from sqlalchemy.ext.asyncio import AsyncSession

from pathlib import Path

import pytest
import os


@pytest.mark.asyncio
async def test_count(session: AsyncSession):
    DATA_PATH_1 = Path("tests") / "integration" / "data" / "california_housing.MedInc1.csv"
    DATA_PATH_2 = Path("tests") / "integration" / "data" / "california_housing.MedInc2.csv"
    DATA_PATH_3 = Path("tests") / "integration" / "data" / "california_housing.validation.csv"

    assert os.path.exists(DATA_PATH_1)
    assert os.path.exists(DATA_PATH_2)
    assert os.path.exists(DATA_PATH_3)

    data1 = DataSourceStorage(
        [
            DataSourceConfiguration(
                name="california1",
                kind="file",
                type="csv",
                path=str(DATA_PATH_1),
                token=[TEST_PROJECT_TOKEN],
            )
        ],
    )
    data2 = DataSourceStorage(
        [
            DataSourceConfiguration(
                name="california2",
                kind="file",
                type="csv",
                path=str(DATA_PATH_2),
                token=[TEST_PROJECT_TOKEN],
            )
        ],
    )
    data3 = DataSourceStorage(
        [
            DataSourceConfiguration(
                name="california3",
                kind="file",
                type="csv",
                path=str(DATA_PATH_3),
                token=[TEST_PROJECT_TOKEN],
            )
        ],
    )

    server = ServerlessExecution(session)
    await server.setup()
    await server.create_project(TEST_PROJECT_TOKEN)

    node1: ServerlessWorker = await server.add_worker(data1)
    node2: ServerlessWorker = await server.add_worker(data2)
    node3: ServerlessWorker = await server.add_worker(data3)

    project = await server.get_project(TEST_PROJECT_TOKEN)

    counter = CountEstimator(
        query=project.extract(),
    )

    art_count = Artifact(
        project_id=project.id,
        steps=counter.get_steps(),
    )

    artifact_id = await server.submit(art_count)

    await assert_jobs_count(server.ar, server.jr, artifact_id, 0, 5, 4, 1, 0, 0)

    await server.self_worker.next_get_execute_post()

    await assert_jobs_count(server.ar, server.jr, artifact_id, 0, 5, 3, 1, 0, 1)

    await node1.next_get_execute_post()

    await assert_jobs_count(server.ar, server.jr, artifact_id, 0, 5, 2, 1, 0, 2)

    await node2.next_get_execute_post()

    await assert_jobs_count(server.ar, server.jr, artifact_id, 0, 5, 1, 1, 0, 3)

    await node3.next_get_execute_post()

    await assert_jobs_count(server.ar, server.jr, artifact_id, 0, 5, 0, 1, 0, 4)

    r_count = await server.self_worker.next_get_execute_post()

    await assert_jobs_count(server.ar, server.jr, artifact_id, 0, 5, 0, 0, 0, 5)

    counter = load_resource(r_count)

    assert counter["count"] == 20640
