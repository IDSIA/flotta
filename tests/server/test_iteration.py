from ferdelance.config import conf
from ferdelance.schemas.models import Model
from ferdelance.schemas.plans import TrainTestSplit, IterativePlan
from ferdelance.schemas.updates import UpdateExecute
from ferdelance.workbench.interface import Artifact

from tests.utils import TEST_PROJECT_TOKEN
from tests.serverless import ServerlessExecution

from sqlalchemy.ext.asyncio import AsyncSession

import logging
import os
import pytest
import shutil
import time

LOGGER = logging.getLogger(__name__)


def start_function(token: str, job_id: str, result_ids: list[str], artifact_id: str) -> str:
    """Pseudo function to simulate the start of an aggregation job."""

    LOGGER.info(
        f"artifact_id={artifact_id}: new aggregation job_id={job_id} with results_ids={result_ids} and token={token}"
    )

    return f"task-worker-{time.time()}"


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
    sse = ServerlessExecution(session)

    await sse.setup(TEST_PROJECT_TOKEN)

    project = await sse.get_project(TEST_PROJECT_TOKEN)
    label: str = project.data.features[0].name

    # artifact creation
    artifact = Artifact(
        project_id=project.project_id,
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

    artifact_id = await sse.submit(artifact)

    await assert_count_it(sse, artifact_id, 0, 1)  # train1

    # ----------------
    # FIRST ITERATION
    # ----------------

    # client
    next_action = await sse.next_action()

    assert isinstance(next_action, UpdateExecute)

    task = await sse.get_client_task(next_action)

    """...simulate client work..."""

    result = await sse.post_client_results(task)

    # server
    can_aggregate = await sse.check_aggregation(result)

    assert can_aggregate

    job = await sse.aggregate(result, start_function)

    await assert_count_it(sse, artifact_id, 0, 2)  # train1 agg1

    # worker

    await sse.get_worker_task(job)

    """...simulate worker aggregation..."""

    await sse.post_worker_result(job)

    # ----------------
    # SECOND ITERATION
    # ----------------

    await assert_count_it(sse, artifact_id, 1, 3)  # train1 agg1 train2

    # client
    next_action = await sse.next_action()

    assert isinstance(next_action, UpdateExecute)

    task = await sse.get_client_task(next_action)

    """...simulate client work..."""

    result = await sse.post_client_results(task)

    # server
    can_aggregate = await sse.check_aggregation(result)

    assert can_aggregate

    job = await sse.aggregate(result, start_function)

    await assert_count_it(sse, artifact_id, 1, 4)  # train1 agg1 train2 agg2

    # worker

    await sse.get_worker_task(job)

    """...simulate worker aggregation..."""

    await sse.post_worker_result(job)

    await assert_count_it(sse, artifact_id, 2, 4)  # train1 agg1 train2 agg2

    assert 1 == len(await sse.ar.list_artifacts())

    # cleanup
    shutil.rmtree(os.path.join(conf.STORAGE_ARTIFACTS, artifact_id))
