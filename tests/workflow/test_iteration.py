from ferdelance.config import config_manager
from ferdelance.core.distributions.many import Collect
from ferdelance.core.model_operations import Train, TrainTest, Aggregation
from ferdelance.core.steps import Iterate, Finalize, Parallel
from ferdelance.core.transformers import FederatedSplitter
from ferdelance.logging import get_logger
from ferdelance.schemas.updates import UpdateData
from ferdelance.shared.status import JobStatus
from ferdelance.workbench.interface import Artifact

from tests.dummies import DummyModel
from tests.serverless import ServerlessExecution
from tests.utils import TEST_PROJECT_TOKEN, get_metadata

from sqlalchemy.ext.asyncio import AsyncSession

import os
import pytest
import shutil

LOGGER = get_logger(__name__)


async def assert_count_it(
    sse: ServerlessExecution,
    artifact_id: str,
    exp_iteration: int,
    exp_jobs_count: int = 0,
    exp_jobs_waiting: int = 0,
    exp_jobs_scheduled: int = 0,
    exp_jobs_running: int = 0,
    exp_jobs_completed: int = 0,
    exp_jobs_failed: int = 0,
) -> None:
    ar_db = await sse.ar.get_artifact(artifact_id)

    jobs_count = await sse.jr.count_jobs_by_artifact_id(artifact_id)

    job_waiting_count = await sse.jr.count_jobs_by_artifact_status(artifact_id, JobStatus.WAITING)
    job_scheduled_count = await sse.jr.count_jobs_by_artifact_status(artifact_id, JobStatus.SCHEDULED)
    job_running_count = await sse.jr.count_jobs_by_artifact_status(artifact_id, JobStatus.RUNNING)
    job_completed_count = await sse.jr.count_jobs_by_artifact_status(artifact_id, JobStatus.COMPLETED)
    job_failed_count = await sse.jr.count_jobs_by_artifact_status(artifact_id, JobStatus.ERROR)

    print("=" * 32)
    print("iteration:     ", ar_db.iteration, "(", exp_iteration, ")")
    print("jobs count:    ", jobs_count, "(", exp_jobs_count, ")")
    print("jobs waiting:  ", job_waiting_count, "(", exp_jobs_waiting, ")")
    print("jobs scheduled:", job_scheduled_count, "(", exp_jobs_scheduled, ")")
    print("jobs running:  ", job_running_count, "(", exp_jobs_running, ")")
    print("jobs completed:", job_completed_count, "(", exp_jobs_completed, ")")
    print("jobs failed:   ", job_failed_count, "(", exp_jobs_failed, ")")
    print("=" * 32)

    assert ar_db.iteration == exp_iteration
    assert jobs_count == exp_jobs_count
    assert job_waiting_count == exp_jobs_waiting
    assert job_scheduled_count == exp_jobs_scheduled
    assert job_running_count == exp_jobs_running
    assert job_completed_count == exp_jobs_completed
    assert job_failed_count == exp_jobs_failed


@pytest.mark.asyncio
async def test_iteration(session: AsyncSession):
    server = ServerlessExecution(session)

    await server.setup()
    await server.create_project(TEST_PROJECT_TOKEN)

    worker = await server.add_worker(1, get_metadata(TEST_PROJECT_TOKEN))

    project = await server.get_project(TEST_PROJECT_TOKEN)
    label: str = project.data.features[0].name

    # artifact creation
    model = DummyModel()
    artifact = Artifact(
        id="",
        project_id=project.id,
        steps=[
            Iterate(
                iterations=2,
                steps=[
                    Parallel(
                        TrainTest(
                            query=project.extract().add(
                                FederatedSplitter(
                                    random_state=42,
                                    test_percentage=0.5,
                                    label=label,
                                )
                            ),
                            trainer=Train(model=model),
                            model=model,
                        ),
                        Collect(),
                    ),
                    Finalize(Aggregation(model=model)),
                ],
            )
        ],
    )

    artifact_id = await server.submit(artifact)

    # ----------------
    # FIRST ITERATION
    # ----------------

    await assert_count_it(server, artifact_id, 0, 4, 3, 1, 0, 0)  # train1

    # client
    next_action = await worker.next_action()

    assert isinstance(next_action, UpdateData)
    assert next_action.action == "EXECUTE"

    task = await server.get_task(next_action.job_id)

    await assert_count_it(server, artifact_id, 0, 4, 3, 0, 1, 0)  # train1

    """...simulate client work..."""

    await server.task_completed(task)

    await assert_count_it(server, artifact_id, 0, 4, 2, 1, 0, 1)  # train1 agg1

    # server
    job_id = await server.next(server.self_component)

    assert job_id is not None

    task = await server.get_task(job_id)

    await assert_count_it(server, artifact_id, 0, 4, 2, 0, 1, 1)  # train1 agg1

    """...simulate work on the server..."""

    await server.task_completed(task)

    # ----------------
    # SECOND ITERATION
    # ----------------

    await assert_count_it(server, artifact_id, 1, 4, 1, 1, 0, 2)  # train1 agg1 train2

    # client
    next_action = await worker.next_action()

    assert isinstance(next_action, UpdateData)
    assert next_action.action == "EXECUTE"

    task = await worker.get_task(next_action)

    await assert_count_it(server, artifact_id, 1, 4, 1, 0, 1, 2)  # train1 agg1 train2

    """...simulate client work..."""

    await server.task_completed(task)

    await assert_count_it(server, artifact_id, 1, 4, 0, 1, 0, 3)  # train1 agg1 train2 agg2

    # server
    job_id = await server.next(server.self_component)

    assert job_id is not None

    task = await server.get_task(job_id)

    await assert_count_it(server, artifact_id, 1, 4, 0, 0, 1, 3)  # train1 agg1 train2 agg2

    """...simulate work on the server..."""

    await server.task_completed(task)

    await assert_count_it(server, artifact_id, 1, 4, 0, 0, 0, 4)  # train1 agg1 train2 agg2

    assert 1 == len(await server.ar.list_artifacts())

    # client
    next_action = await worker.next_action()

    assert isinstance(next_action, UpdateData)
    assert next_action.action == "DO_NOTHING"
    assert next_action.job_id == ""

    # cleanup
    shutil.rmtree(os.path.join(config_manager.get().storage_artifact(artifact_id)))
