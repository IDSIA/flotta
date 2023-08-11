from ferdelance.config import config_manager, get_logger
from ferdelance.schemas.models import Model
from ferdelance.schemas.plans import TrainTestSplit, IterativePlan
from ferdelance.schemas.updates import UpdateExecute
from ferdelance.schemas.tasks import TaskArguments
from ferdelance.workbench.interface import Artifact

from tests.utils import TEST_PROJECT_TOKEN, get_metadata
from tests.serverless import ServerlessExecution

from sqlalchemy.ext.asyncio import AsyncSession

import os
import pytest
import shutil

LOGGER = get_logger(__name__)


def start_function(args: TaskArguments) -> None:
    """Pseudo function to simulate the start of an aggregation job."""

    LOGGER.info(f"artifact_id={args.artifact_id}: new aggregation job_id={args.job_id} with token={args.token}")


async def assert_count_it(sse: ServerlessExecution, artifact_id: str, exp_iteration: int, exp_jobs: int) -> None:
    ar_db = await sse.ar.get_artifact(artifact_id)

    job_count = await sse.jr.count_jobs_by_artifact_id(artifact_id)

    print("=" * 32)
    print("iteration:", ar_db.iteration, "(", exp_iteration, ")")
    print("job_count:", job_count, "(", exp_jobs, ")")
    print("=" * 32)

    assert ar_db.iteration == exp_iteration
    assert job_count == exp_jobs


@pytest.mark.asyncio
async def test_iteration(session: AsyncSession):
    server = ServerlessExecution(session)

    await server.setup()
    await server.create_project(TEST_PROJECT_TOKEN)

    client = await server.add_client(1, get_metadata())

    project = await server.get_project(TEST_PROJECT_TOKEN)
    label: str = project.data.features[0].name

    # artifact creation
    artifact = Artifact(
        project_id=project.id,
        transform=project.extract(),
        model=Model(name="model", strategy=""),
        plan=IterativePlan(
            iterations=2,
            local_plan=TrainTestSplit(
                label=label,
                test_percentage=0.5,
            ),
        ).build(),
    )

    artifact_id = await server.submit(artifact)

    await assert_count_it(server, artifact_id, 0, 1)  # train1

    # ----------------
    # FIRST ITERATION
    # ----------------

    # client
    next_action = await client.next_action()

    assert isinstance(next_action, UpdateExecute)

    task = await client.get_client_task(next_action.job_id)

    """...simulate client work..."""

    result = await client.post_client_results(task)

    # server
    can_aggregate = await server.check_aggregation(result)

    assert can_aggregate

    job = await server.aggregate(result, start_function)

    await assert_count_it(server, artifact_id, 0, 2)  # train1 agg1

    # worker

    await server.get_worker_task(job)

    """...simulate worker aggregation..."""

    await server.post_worker_result(job)

    # ----------------
    # SECOND ITERATION
    # ----------------

    await assert_count_it(server, artifact_id, 1, 3)  # train1 agg1 train2

    # client
    next_action = await client.next_action()

    assert isinstance(next_action, UpdateExecute)

    task = await client.get_client_task(next_action.job_id)

    """...simulate client work..."""

    result = await client.post_client_results(task)

    # server
    can_aggregate = await server.check_aggregation(result)

    assert can_aggregate

    job = await server.aggregate(result, start_function)

    await assert_count_it(server, artifact_id, 1, 4)  # train1 agg1 train2 agg2

    # worker

    await server.get_worker_task(job)

    """...simulate worker aggregation..."""

    await server.post_worker_result(job)

    await assert_count_it(server, artifact_id, 2, 4)  # train1 agg1 train2 agg2

    assert 1 == len(await server.ar.list_artifacts())

    # cleanup
    shutil.rmtree(os.path.join(config_manager.get().storage_artifact(artifact_id)))
