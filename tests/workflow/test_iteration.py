from ferdelance.config import config_manager
from ferdelance.core.distributions import Collect
from ferdelance.core.interfaces import Iterate
from ferdelance.core.model_operations import Train, TrainTest, Aggregation
from ferdelance.core.steps import Finalize, Parallel
from ferdelance.core.transformers import FederatedSplitter
from ferdelance.logging import get_logger
from ferdelance.schemas.updates import UpdateData
from ferdelance.workbench.interface import Artifact

from tests.dummies import DummyModel
from tests.serverless import ServerlessExecution
from tests.utils import TEST_PROJECT_TOKEN, assert_jobs_count, get_metadata

from sqlalchemy.ext.asyncio import AsyncSession

import pytest
import shutil

LOGGER = get_logger(__name__)


@pytest.mark.asyncio
async def test_iteration(session: AsyncSession):
    server = ServerlessExecution(session)

    await server.setup()
    await server.create_project(TEST_PROJECT_TOKEN)

    worker = await server.add_worker(get_metadata(TEST_PROJECT_TOKEN))

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

    await assert_jobs_count(server.ar, server.jr, artifact_id, 0, 4, 3, 1, 0, 0)  # train1

    # client
    next_action = await worker.next_action()

    assert isinstance(next_action, UpdateData)
    assert next_action.action == "EXECUTE"

    task = await server.get_task(next_action.job_id)

    await assert_jobs_count(server.ar, server.jr, artifact_id, 0, 4, 3, 0, 1, 0)  # train1

    """...simulate client work..."""

    await server.task_completed(task)

    await assert_jobs_count(server.ar, server.jr, artifact_id, 0, 4, 2, 1, 0, 1)  # train1 agg1

    # server
    job_id = await server.next(server.self_component)

    assert job_id is not None

    task = await server.get_task(job_id)

    await assert_jobs_count(server.ar, server.jr, artifact_id, 0, 4, 2, 0, 1, 1)  # train1 agg1

    """...simulate work on the server..."""

    await server.task_completed(task)

    # ----------------
    # SECOND ITERATION
    # ----------------

    await assert_jobs_count(server.ar, server.jr, artifact_id, 1, 4, 1, 1, 0, 2)  # train1 agg1 train2

    # client
    next_action = await worker.next_action()

    assert isinstance(next_action, UpdateData)
    assert next_action.action == "EXECUTE"

    task = await worker.get_task(next_action)

    await assert_jobs_count(server.ar, server.jr, artifact_id, 1, 4, 1, 0, 1, 2)  # train1 agg1 train2

    """...simulate client work..."""

    await server.task_completed(task)

    await assert_jobs_count(server.ar, server.jr, artifact_id, 1, 4, 0, 1, 0, 3)  # train1 agg1 train2 agg2

    # server
    job_id = await server.next(server.self_component)

    assert job_id is not None

    task = await server.get_task(job_id)

    await assert_jobs_count(server.ar, server.jr, artifact_id, 1, 4, 0, 0, 1, 3)  # train1 agg1 train2 agg2

    """...simulate work on the server..."""

    await server.task_completed(task)

    await assert_jobs_count(server.ar, server.jr, artifact_id, 1, 4, 0, 0, 0, 4)  # train1 agg1 train2 agg2

    assert 1 == len(await server.ar.list_artifacts())

    # client
    next_action = await worker.next_action()

    assert isinstance(next_action, UpdateData)
    assert next_action.action == "DO_NOTHING"
    assert next_action.job_id == ""

    # cleanup
    shutil.rmtree(config_manager.get().storage_artifact(artifact_id))
