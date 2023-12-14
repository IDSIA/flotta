from typing import Any
from ferdelance.config import DataSourceConfiguration, DataSourceStorage
from ferdelance.core.artifacts import Artifact
from ferdelance.core.distributions import Collect
from ferdelance.core.model_operations import Train, TrainTest, Aggregation
from ferdelance.core.models import FederatedRandomForestClassifier, StrategyRandomForestClassifier
from ferdelance.core.steps import Finalize, Parallel
from ferdelance.core.transformers import FederatedSplitter
from ferdelance.database import AsyncSession
from ferdelance.schemas.database import Resource

from tests.serverless import ServerlessExecution
from tests.utils import TEST_PROJECT_TOKEN, assert_jobs_count

from sklearn.ensemble import RandomForestClassifier

from pathlib import Path

import os
import pickle
import pytest


def load_resource(res: Resource) -> Any:
    with open(res.path, "rb") as f:
        return pickle.load(f)


@pytest.mark.asyncio
async def test_aggregation(session: AsyncSession):
    DATA_PATH_1 = Path("tests") / "integration" / "data" / "california_housing.MedInc1.csv"
    DATA_PATH_2 = Path("tests") / "integration" / "data" / "california_housing.MedInc2.csv"

    assert os.path.exists(DATA_PATH_1)
    assert os.path.exists(DATA_PATH_2)

    data1 = DataSourceStorage(
        [
            DataSourceConfiguration(
                name="california1",
                token=[TEST_PROJECT_TOKEN],
                kind="file",
                type="csv",
                path=str(DATA_PATH_1),
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

    server = ServerlessExecution(session)
    await server.setup()

    await server.create_project(TEST_PROJECT_TOKEN)

    client1 = await server.add_worker(data1)
    client2 = await server.add_worker(data2)

    clients = [c.id for c in await server.cr.list_clients()]
    nodes = [n.id for n in await server.cr.list_nodes()]

    assert len(clients) == 2
    assert client1.component.id in clients
    assert client2.component.id in clients

    assert len(nodes) == 1
    assert server.self_worker.component.id in nodes

    # submit artifact

    project = await server.get_project(TEST_PROJECT_TOKEN)
    label = "MedHouseValDiscrete"

    model = FederatedRandomForestClassifier(
        n_estimators=10,
        strategy=StrategyRandomForestClassifier.MERGE,
    )

    artifact: Artifact = Artifact(
        project_id=project.id,
        steps=[
            Parallel(
                TrainTest(
                    query=project.extract().add(
                        FederatedSplitter(
                            random_state=42,
                            test_percentage=0.2,
                            label=label,
                        )
                    ),
                    trainer=Train(model=model),
                    model=model,
                ),
                Collect(),
            ),
            Finalize(
                Aggregation(model=model),
            ),
        ],
    )

    artifact_id = await server.submit(artifact)

    jobs = await server.jr.list_jobs_by_artifact_id(artifact.id)

    assert len(jobs) == 3

    await assert_jobs_count(server.ar, server.jr, artifact_id, 0, 3, 1, 2, 0, 0)

    # client 1
    res1 = await client1.next_get_execute_post()

    await assert_jobs_count(server.ar, server.jr, artifact_id, 0, 3, 1, 1, 0, 1)

    local_model_1 = load_resource(res1)["model"]

    assert isinstance(local_model_1, RandomForestClassifier)
    assert local_model_1.n_estimators == 10  # type: ignore

    job_id = await server.next(server.self_component)

    assert job_id is None

    # client 2
    res2 = await client2.next_get_execute_post()

    await assert_jobs_count(server.ar, server.jr, artifact_id, 0, 3, 0, 1, 0, 2)

    local_model_2 = load_resource(res2)["model"]

    assert isinstance(local_model_2, RandomForestClassifier)
    assert local_model_2.n_estimators == 10  # type: ignore

    # server
    job_id = await server.next(server.self_component)

    assert job_id is not None

    res3 = await server.self_worker.next_get_execute_post()

    await assert_jobs_count(server.ar, server.jr, artifact_id, 0, 3, 0, 0, 0, 3)

    model_agg = load_resource(res3)["model"]

    assert isinstance(model_agg, RandomForestClassifier)
    assert model_agg.n_estimators == 20  # type: ignore

    # check
    next_action = await client1.next_action()

    assert next_action.action == "DO_NOTHING"

    next_action = await client2.next_action()

    assert next_action.action == "DO_NOTHING"

    job_id = await server.next(server.self_component)

    assert job_id is None
