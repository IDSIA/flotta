from ferdelance.core.estimators import CountEstimator
from ferdelance.workbench.interface import Artifact

from tests.utils import TEST_PROJECT_TOKEN, assert_jobs_count, get_metadata
from tests.serverless import ServerlessExecution, ServerlessWorker

from sqlalchemy.ext.asyncio import AsyncSession

import pytest

from hashlib import sha256
from uuid import uuid5, NAMESPACE_URL

from tests.workflow.test_aggregation import load_resource


@pytest.mark.asyncio
async def test_count(session: AsyncSession):
    server = ServerlessExecution(session)
    await server.setup()
    await server.create_project(TEST_PROJECT_TOKEN)

    ds_id_1: str = str(uuid5(NAMESPACE_URL, "ds_1"))
    ds_id_2: str = str(uuid5(NAMESPACE_URL, "ds_2"))
    ds_id_3: str = str(uuid5(NAMESPACE_URL, "ds_3"))

    ds_hs_1: str = sha256(ds_id_1.encode()).hexdigest()
    ds_hs_2: str = sha256(ds_id_2.encode()).hexdigest()
    ds_hs_3: str = sha256(ds_id_3.encode()).hexdigest()

    node1: ServerlessWorker = await server.add_worker(get_metadata(TEST_PROJECT_TOKEN, ds_id_1, ds_hs_1))
    node2: ServerlessWorker = await server.add_worker(get_metadata(TEST_PROJECT_TOKEN, ds_id_2, ds_hs_2))
    node3: ServerlessWorker = await server.add_worker(get_metadata(TEST_PROJECT_TOKEN, ds_id_3, ds_hs_3))

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

    print(counter)
