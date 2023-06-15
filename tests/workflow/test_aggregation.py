from ferdelance.database import AsyncSession
from ferdelance.client.datasources import DataSourceFile
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.updates import UpdateExecute, UpdateNothing

from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.models import (
    FederatedRandomForestClassifier,
    StrategyRandomForestClassifier,
    ParametersRandomForestClassifier,
)
from ferdelance.schemas.plans import TrainTestSplit

from tests.serverless import ServerlessExecution
from tests.utils import TEST_PROJECT_TOKEN

import os
import pytest


@pytest.mark.asyncio
async def test_aggregation(session: AsyncSession):
    DATA_PATH_1 = os.path.join("tests", "integration", "data", "california_housing.MedInc1.csv")
    DATA_PATH_2 = os.path.join("tests", "integration", "data", "california_housing.MedInc2.csv")

    assert os.path.exists(DATA_PATH_1)
    assert os.path.exists(DATA_PATH_2)

    ds1 = DataSourceFile("california1", "csv", DATA_PATH_1, [TEST_PROJECT_TOKEN])
    ds2 = DataSourceFile("california2", "csv", DATA_PATH_1, [TEST_PROJECT_TOKEN])

    server = ServerlessExecution(session)
    await server.setup()

    await server.create_project(TEST_PROJECT_TOKEN)

    client1 = await server.add_client(1, Metadata(datasources=[ds1.metadata()]))
    client2 = await server.add_client(2, Metadata(datasources=[ds2.metadata()]))

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
    next_action = await client1.next_action()

    assert isinstance(next_action, UpdateExecute)

    task = await client1.get_client_task(next_action)

    result = await client1.post_client_results(task)
    can_aggregate = await client1.check_aggregation(result)

    assert not can_aggregate

    # client 2
    next_action = await client2.next_action()

    assert isinstance(next_action, UpdateExecute)

    task = await client2.get_client_task(next_action)
    result = await client2.post_client_results(task)
    can_aggregate = await client2.check_aggregation(result)

    assert can_aggregate

    # check
    next_action = await client1.next_action()

    assert isinstance(next_action, UpdateNothing)

    next_action = await client2.next_action()

    assert isinstance(next_action, UpdateNothing)
